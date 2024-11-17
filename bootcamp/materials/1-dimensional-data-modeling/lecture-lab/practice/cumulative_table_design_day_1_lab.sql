select
	*
from
	player_seasons ps;

-- Create a composite type to represent season statistics
create type season_stats as (
							season INTEGER, -- the year
							gp INTEGER,     -- games played
							pts real,       -- points scored
							reb real,       -- rebounds made 
							ast real        -- assists made
							
);

-- Create an enumerated type to classify player performance based on scoring
create type scoring_class as enum (
									'star',    -- high-performing player (e.g., > 20 points)
									'good',    -- above average player (e.g., 15-20 points)
									'average', -- average player (e.g., 10-15 points)
									'bad'      -- low-performing player (e.g., < 10 points)
);

create table players (
					player_name text,
					height text,
					college text,
					country text,
					draft_year text,
					draft_round text,
					draft_number text,
					season_stats season_stats[],
					scoring_class scoring_class,
					years_since_last_season INTEGER,
					current_season INTEGER,
					primary key(player_name,
current_season) -- composite primary key on player name and current season
					);

-- Select the minimum season from the player_seasons table
select MIN(season) from player_seasons ps;

insert into players
	-- Define Common Table Expressions (CTEs) to gather data for two different seasons (adjust current_season and season values to load more datasets as a pipeline)
with yesterday as (
	select *
	from players
	where current_season = 2000
	),
	today as (
	select *
	from player_seasons
	where season = 2001
	)

select
	-- Use COALESCE to get player_name, prioritizing today's data
		coalesce(t.player_name,
	y.player_name) as player_name,
	-- Use COALESCE to get height, prioritizing today's data
		coalesce(t.height,
	y.height) as height,
	-- Use COALESCE to get college, prioritizing today's data
		coalesce(t.college,
	y.college) as college,
	-- Use COALESCE to get country, prioritizing today's data
		coalesce(t.country,
	y.country) as country,
	-- Use COALESCE to get draft_year, prioritizing today's data
		coalesce(t.draft_year,
	y.draft_year) as draft_year,
	-- Use COALESCE to get draft_round, prioritizing today's data
		coalesce(t.draft_round,
	y.draft_round) as draft_round,
	-- Use COALESCE to get draft_number, prioritizing today's data
		coalesce(t.draft_number,
	y.draft_number) as draft_number,
		/* 
       Use CASE to handle season statistics:
       If y.season_stats is NULL, create an initial array with the current season's stats from today's data.
    */
		case
		when y.season_stats is null
			then array[row(
						t.season,
						t.gp,
						t.pts,
						t.reb,
						t.ast
						)::season_stats]
		-- If today's season is not NULL, append today's stats to the existing season_stats from yesterday.
		when t.season is not null then y.season_stats || array[row(
						t.season,
						t.gp,
						t.pts,
						t.reb,
						t.ast
						)::season_stats]
		-- Otherwise, carry forward the existing season_stats (e.g., for retired players).
		else y.season_stats
	end as season_stats,
	/*
Determine the scoring classification based on points scored in the current season:
If today's season data exists, classify based on points:
       - More than 20 points: 'star'
       - More than 15 points: 'good'
       - More than 10 points: 'average'
       - 10 points or fewer: 'bad'
If today's data is not available, use yesterday's scoring_class.
 */
	case 
		when t.season is not null then 
		case
			when t.pts > 20 then 'star'
			when t.pts > 15 then 'good'
			when t.pts > 10 then 'average'
			else 'bad'
		end::scoring_class
		else y.scoring_class
	end as scoring_class,
	/*
Calculate the number of years since the last active season:
If today's season data exists, set years_since_last_season to 0, 
indicating the player is currently active.
If today's data is not available, increment years_since_last_season from yesterday's data by 1.
 */
	case
		when t.season is not null then 0
		else y.years_since_last_season + 1
	end as years_since_last_season,
	-- Use COALESCE to determine the current season, defaulting to yesterday's current season plus one if today's data is NULL
		coalesce(t.season,
	y.current_season + 1) as current_season
	-- Specify the 'today' CTE as the main source
from
	today t
	-- Perform a FULL OUTER JOIN to combine records from both CTEs
full outer join yesterday y
		on
	t.player_name = y.player_name
/* 
  FULL OUTER JOIN is used to ensure that we include all players from both seasons (1995 and 1996).
  This is important because we want to capture players who may only have data in one of the 
  tables, whether they were active in 1995 or 1996, or both.
    */
	;
-- Directly selects and unnests the season_stats for the current season and player
select
	player_name,
	-- Unnest the season_stats array into separate rows, casting to the season_stats type
	unnest(season_stats)::season_stats as season_stats
from
	players
where
	current_season = 2001
	and player_name = 'Allen Iverson'
/*
 It will return the player's season statistics for the season year,
 with each entry in season_stats presented as a separate row.
 Usage example:
- When you need a quick, simple result and don’t require further processing of the data.
- When the output structure does not need to be altered or expanded.
*/
	-- Uses a Common Table Expression (CTE) to first unnest the season_stats
with unnested as (
	-- Select player_name and unnest season_stats in the CTE
	select
		player_name,
		-- Unnest the season_stats array, casting to the season_stats type
		unnest(season_stats)::season_stats as season_stats
	from
		players
	where
		current_season = 2001
)
	-- Final selection from the CTE
select
	player_name,
	-- Expand the season_stats to return all attributes as separate columns
		(season_stats::season_stats).*
from
	unnested
/*
It will return the player's season statistics for the season year,
with each field in season_stats (e.g., year, games played, points, etc.) as separate columns.
Usage example:
- When you need to perform additional operations on the unnested data or if you want a cleaner structure in the final output.
- When the SQL query is complex, and breaking it into parts (with CTEs) improves clarity and maintainability.
*/
	
select 
	player_name,
	-- Calculate the ratio of points scored in the most recent season to the points scored in the first season
		(season_stats[cardinality(season_stats)]::season_stats).pts /
	case
		-- Check if the points scored in the first season (first element of the array) is zero
		when (season_stats[1]).pts = 0 then 1
		-- If it's zero, use 1 to avoid division by zero
		else
		-- Otherwise, use the points scored in the first season
		(season_stats[1]::season_stats).pts
	end
from
	players
where
	current_season = 2001;
