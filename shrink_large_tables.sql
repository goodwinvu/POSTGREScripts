DO $$
DECLARE
		mviews RECORD;
		rectext text;
		tsize text;
        recindx text;
        beg_tsize text;
		short_name text;
		big_size int;
		num_row_save int;
		sDate timestamp;
		only_test bool;
BEGIN
	big_size := 20971520; -- указываем размер после которого таблица считается большй 20мб по умолчанию
	sDate := '2024-02-01'; -- указываем период до которого обрезаем периодические таблицы
   	num_row_save := 100; -- количество строк оставляемых в непериодических таблицах
	only_test = true; -- тестовый прогон - true - не трогать данные, только сформировать выборки, false - удаление данных
	
	RAISE NOTICE 'Очистка больших таблиц.Начало выполнения.
    ';

    FOR mviews IN
       SELECT
		table_name,
		column_name,
		table_size AS table_size,
		pg_size_pretty(indexes_size) AS indexes_size,
		pg_size_pretty(total_size) AS total_size
		FROM (
			SELECT DISTINCT
			table_name AS table_name,
			column_name AS column_name,
			pg_table_size(table_name) AS table_size,
			pg_indexes_size(table_name) AS indexes_size,
			pg_total_relation_size(table_name) AS total_size
			FROM (SELECT ('"' || tb.table_schema || '"."' || tb.table_name || '"') AS table_name,
					cl.column_name AS column_name
			FROM information_schema.tables AS tb
			LEFT JOIN information_schema.columns AS cl
			 ON tb.table_name = cl.table_name and cl.data_type = 'timestamp without time zone'
			WHERE tb.table_name <> 'config' 
			) AS all_tables
		ORDER BY total_size DESC
		) AS pretty_sizes
		WHERE table_size > big_size AND 
			  table_name NOT LIKE '%_referenc%' AND
			  table_name NOT LIKE '%pg_%'  AND
			  table_name NOT LIKE '%params%'
    LOOP
		RAISE NOTICE 'Очистка таблицы % ...', mviews.table_name;
		
		-- Подсчет начального места
		EXECUTE 'SELECT pg_size_pretty( pg_table_size( '''|| mviews.table_name ||''' ) )' Into beg_tsize;
        RAISE NOTICE 'Начальный размер таблицы % ', beg_tsize;
		-- Очистка таблиц
		IF  mviews.column_name IS NULL 
			THEN
				--rectext := 'TRUNCATE ' || mviews.table_name; раньше удаляли целиком  
				EXECUTE 'SELECT replace('''|| mviews.table_name ||''', ''"'', '''')'  Into short_name;
				rectext := 'DELETE FROM ' || mviews.table_name || '	WHERE ctid IN (SELECT ctid FROM ' || mviews.table_name || ' ORDER BY ctid asc
                         LIMIT (	SELECT reltuples::bigint AS estimate 
									FROM   pg_class WHERE  oid = '''|| short_name ||'''::regclass) - ' || num_row_save || ')'; -- теперь оставляем 100 записей
			ELSE
			    rectext := 'DELETE FROM ' || mviews.table_name || ' WHERE ' || mviews.column_name || ' < timestamp '''||sDate||''''; 
		END IF;
				
		IF only_test = true THEN
				RAISE NOTICE 'Выполнится очистка %',rectext;
		ELSE
			BEGIN
				EXECUTE (rectext);
					-- Подсчет нового места
				EXECUTE 'SELECT pg_size_pretty( pg_table_size((tb.table_schema||''.''||tb.table_name ))) 
							FROM information_schema.tables AS tb
							WHERE  tb.table_name = split_part(replace( '''||mviews.table_name||''', ''"'', ''''),''.'',2)' Into tsize;

				IF  mviews.column_name IS NULL THEN
					RAISE NOTICE 'Очищена таблица % , оставлено 100 записей, нач.размер % - итог.размер % .',
					mviews.table_name, beg_tsize, tsize;	
				ELSE
				RAISE NOTICE 'Очищена таблица % по условию период записи % < %, нач.размер % - итог.размер % .',
				mviews.table_name, mviews.column_name, sDate, beg_tsize, tsize;	
END IF;
		
			EXCEPTION WHEN OTHERS 
				THEN
				RAISE NOTICE 'ERROR CODE: %. MESSAGE TEXT: %', SQLSTATE, SQLERRM;
				RAISE NOTICE 'Ошибка очистки таблицы % 
                Запрос - %
				', mviews.table_name, rectext;
			END;
		END IF;	
		
		-- Очистка индексов
		IF only_test = false THEN
			RAISE NOTICE 'Очистка индексов таблицы % ...', mviews.table_name;

			BEGIN
				EXECUTE (SELECT 'DROP INDEX ' || string_agg(indexrelid::regclass::text, ', ')
				FROM   pg_index  i
				WHERE  i.indrelid = mviews.table_name::regclass );
				RAISE NOTICE 'Таблица % очищена ', mviews.table_name;
			EXCEPTION WHEN null_value_not_allowed THEN
                        RAISE NOTICE 'Индекса таблицы % была очищена ранее', mviews.table_name;
                      WHEN OTHERS THEN
				        RAISE NOTICE 'ERROR CODE: %. MESSAGE TEXT: %', SQLSTATE, SQLERRM;
				        RAISE NOTICE 'Ошибка очистки индекса таблицы % 
				', mviews.table_name;
			END;
		END IF;
    END LOOP;
    RAISE NOTICE '
    Очистка завершена...';	
END $$