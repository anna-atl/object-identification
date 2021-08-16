import pandas as pd
import numpy as np
import time
import datetime

wordbook_name_1 = "~/Dropbox/Botva/TUM/Master_Thesis/object-identification/df_matches_full_url_clean_2021-07-29_23-52-20-319138.csv"
print('Started to import from', wordbook_name_1)
df_1 = pd.read_csv(wordbook_name_1, encoding='latin-1', sep=',', error_bad_lines=False)  # error_bad_lines=False skips bad data

df_1 = df_1[['name', 'country', 'city', 'zip', 'street', 'url']]
df_1['datasource'] = 'france_rna'
df = df_1
del df_1



CREATE TABLE labeled_data AS
select *
from
(
		select  doc_1, doc_2
			, name_x, country_x, url_x
			, name_y, country_y, url_y
			, num_matches
			, max_match_score
			, row_number() over(partition by num_matches order by max_match_score desc) rn
		from
		(
		select distinct doc_1, doc_2
			, name_x, country_x, url_x
			, name_y, country_y, url_y
			, max(match_score) over(partition by  doc_1, doc_2)  max_match_score
			, count(*) over(partition by  doc_1, doc_2) num_matches
		FROM
		(
			select match_score, doc_1, doc_2
			, name_x, country_x, url_x
			, name_y, country_y, url_y
			from "df_matches_full_url_clean_2021-07-29 23:52:20"
			where match_score != 1.0
			UNION all
			select match_score, doc_1, doc_2
			, name_x, country_x, url_x
			, name_y, country_y, url_y
			from "df_matches_full_url_clean_2021-07-29 23:59:08"
			where match_score != 1.0
			UNION all
			select match_score, doc_1, doc_2
			, name_x, country_x, url_x
			, name_y, country_y, url_y
			from "df_matches_full_url_clean_2021-07-30 00:05:16"
			where match_score != 1.0
		)
	)
)
where rn <= 100 and num_matches >1
order by num_matches desc, rn desc











insert into labeled_data
select *
from
(
		select  doc_1, doc_2
			, name_x, country_x, url_x
			, name_y, country_y, url_y
			, num_matches
			, max_match_score
			, row_number() over(partition by num_matches order by max_match_score desc) rn
			, 'multiple' source_name
			, 'url_clean' matched_attribute
		from
		(
		select distinct doc_1, doc_2
			, name_x, country_x, url_x
			, name_y, country_y, url_y
			, max(match_score) over(partition by  doc_1, doc_2)  max_match_score
			, count(*) over(partition by  doc_1, doc_2) num_matches
		FROM
		(
			select match_score, doc_1, doc_2
			, name_x, country_x, url_x
			, name_y, country_y, url_y
			from "df_matches_full_url_clean_2021-07-29 23:38:47"
			where match_score != 1.0
		)
	)
)
where rn <= 100 and num_matches >1
order by num_matches desc, rn desc







insert into labeled_data
select *
from
(
		select  doc_1, doc_2
			, name_x, country_x, url_x
			, name_y, country_y, url_y
			, num_matches
			, max_match_score
			, row_number() over(partition by num_matches order by max_match_score desc) rn
			, 'multiple' source_name
			, 'name_clean' matched_attribute
			, '2021-07-31' snapshot_datetime
		from
		(
		select distinct doc_1, doc_2
			, name_x, country_x, url_x
			, name_y, country_y, url_y
			, max(match_score) over(partition by  doc_1, doc_2)  max_match_score
			, count(*) over(partition by  doc_1, doc_2) num_matches
		FROM
		(
			select match_score, doc_1, doc_2
			, name_x, country_x, url_x
			, name_y, country_y, url_y
			from "df_matches_full_name_clean_2021-07-29 16:07:43"
			where match_score != 1.0
		)
	)
)
where rn <= 100 and num_matches >1
order by num_matches desc, rn desc