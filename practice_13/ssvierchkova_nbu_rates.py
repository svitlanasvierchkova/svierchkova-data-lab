from datetime import date, datetime, timedelta

from airflow.sdk import BaseHook, Variable, dag, task

STUDENT = "svitlanasvierchkova"  # ваш GitHub-логін маленькими літерами
PREFIX = "svierchkova_svitlana"  # ваша папка в Blob Storage: prizvysche_imya

ENDPOINT = "/NBUStatService/v1/statdirectory/exchange"
# без User-Agent НБУ інколи відповідає 403
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36"
}


def fetch_rates(day: date | None = None) -> list[dict]:
  """Курс на одну дату (None — на сьогодні), лише валюти з Variable."""
  from airflow.providers.http.hooks.http import HttpHook

  # хост і протокол HttpHook бере з Connection nbu_api
  hook = HttpHook(method="GET", http_conn_id="nbu_api")
  params = "json" if day is None else f"date={day:%Y%m%d}&json"
  rates = hook.run(endpoint=f"{ENDPOINT}?{params}", headers=HEADERS).json()

  wanted = Variable.get("nbu_currencies", deserialize_json=True)
  return [
      {
          "currency": r["cc"],
          "rate": float(r["rate"]),
          "rate_date": datetime.strptime(r["exchangedate"], "%d.%m.%Y")
          .date()
          .isoformat(),
      }
      for r in rates
      if r["cc"] in wanted
  ]


@task.branch
def choose_mode() -> str:
  """Читає Variable nbu_load_mode та обирає гілку виконання."""
  mode = Variable.get("nbu_load_mode", default="today")
  print(f"режим: {mode}")
  return "load_period" if mode == "period" else "load_today"


@task
def load_today() -> list[dict]:
  """Завантажує курс валют на сьогодні."""
  rows = fetch_rates()
  print(f"сьогодні: {len(rows)} рядків")
  return rows


@task
def load_period() -> list[dict]:
  """Завантажує курс валют за вказаний період по днях."""
  start = date.fromisoformat(Variable.get("nbu_period_start"))
  end = date.fromisoformat(Variable.get("nbu_period_end"))
  rows = []
  day = start
  while day <= end:
    rows += fetch_rates(day)
    day += timedelta(days=1)
  print(f"період {start} — {end}: {len(rows)} рядків")
  return rows


@task(trigger_rule="none_failed_min_one_success")
def save_report(
    today_rows: list[dict] | None, period_rows: list[dict] | None
) -> None:
  """Збирає отримані дані у DataFrame, формує CSV та завантажує в Azure Blob Storage."""
  import pandas as pd
  from azure.storage.blob import ContainerClient

  rows = today_rows or period_rows or []
  if not rows:
    print("даних немає — нічого не записуємо")
    return

  df = pd.DataFrame(rows).sort_values(["rate_date", "currency"])

  # Формування назви файлу залежно від кількості дат
  first, last = df["rate_date"].min(), df["rate_date"].max()
  suffix = first if first == last else f"{first}_{last}"
  blob_name = f"{PREFIX}/nbu_rates/nbu_rates_{suffix}.csv"

  # Отримання SAS-посилання з Connection
  sas_url = BaseHook.get_connection("blob_lake").password
  lake = ContainerClient.from_container_url(sas_url)

  # Завантаження файлу з перезаписом
  lake.upload_blob(
      name=blob_name, data=df.to_csv(index=False).encode("utf-8"), overwrite=True
  )
  print(f"записано {blob_name}: {len(df)} рядків")

  for blob in lake.list_blobs(name_starts_with=f"{PREFIX}/nbu_rates/"):
    print(f"  {blob.name} {blob.size} байт")


@dag(
    dag_id=f"{STUDENT}_nbu_rates",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["student", STUDENT],
)
def nbu_rates_dag():
  mode = choose_mode()
  t_today = load_today()
  t_period = load_period()

  mode >> [t_today, t_period]
  save_report(t_today, t_period)


nbu_rates_dag()