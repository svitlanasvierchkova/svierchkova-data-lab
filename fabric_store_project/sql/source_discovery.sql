-- Run on source PostgreSQL with read-only access.
SELECT table_name FROM information_schema.tables WHERE table_schema='shop' AND table_type='BASE TABLE' ORDER BY table_name;
SELECT table_name,column_name,data_type,is_nullable FROM information_schema.columns WHERE table_schema='shop' ORDER BY table_name,ordinal_position;
SELECT tc.table_name,kcu.column_name,tc.constraint_type FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name=kcu.constraint_name AND tc.table_schema=kcu.table_schema WHERE tc.table_schema='shop' AND tc.constraint_type IN ('PRIMARY KEY','FOREIGN KEY') ORDER BY tc.table_name;
