	select * from {{ source('staging','for_hire_vecicle')  }}
	where dispatching_base_num is not null
