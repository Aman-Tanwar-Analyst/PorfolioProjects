select * from CovidDeaths
order by 3, 4

select * from CovidVaccinations
order by 3, 4
select location, max(total_cases) as highestinfectedrate, population, max((total_deaths/population))*100 as highestdeathpercentage
  from CovidDeaths
  --where location like 'germany'
  group by location, population 
  order by highestdeathpercentage desc
  
  select date, sum(new_cases) as casesperday -- max(cast(total_deaths as int)) as highestdeathrate
  from CovidDeaths
  --where location like 'germany'
       --where continent is null
  group by date 
  
  
  
select  sum(new_cases) as casesperday, sum(cast(new_deaths as int)) as totaldeaths, sum(cast(new_deaths as int))/sum(new_cases)*100 as death
from CovidDeaths
--where location like 'germany'
--where continent is null
where continent is not null
--group by date 
  order by 1,2
  
  -- joining both tables
*select ded.continent, ded.location, ded.date, ded.population, vac.new_vaccinations, sum(convert(int, vac.new_vaccinations)) over (partition by ded.location order by ded.date, ded. location) as rollingcount
   from CovidDeaths as ded
  join CovidVaccinations as vac
  on ded.location=vac.location
   and ded.date=vac.date
   where ded.continent is not null
   order by 2,3


   with popvsvac ( continent, location, date, population, new_vaccinations, rollingcount)
   as(
   select ded.continent, ded.location, ded.date,ded.population, vac.new_vaccinations,
   sum(convert(int, vac.new_vaccinations)) over (partition by ded.location order by ded.date, ded. location) as rollingcount
   from CovidDeaths as ded
   join CovidVaccinations as vac
   on ded.location=vac.location
   and ded.date=vac.date
   where ded.continent is not null
   --order by 2,3
   )

   select *, (rollingcount/population)*100 as percentbypopl
   from popvsvac

  -- drop table if exists #percentofpopulation
   create table #percentofpopulation
   ( 
   continent varchar(255),
   location varchar(255),
   date datetime,
   population numeric,
   new_vaccinations numeric,
   rollingcount numeric
   )

   insert into #percentofpopulation
   select ded.continent, ded.location, ded.date, ded.population, vac.new_vaccinations, sum(cast(vac.new_vaccinations as int)) over (partition by ded.location order by ded.location, ded.date) as rollingcount
   from CovidDeaths as ded
  join CovidVaccinations as vac
  on ded.location=vac.location
   and ded.date=vac.date
   --where ded.continent is not null
   --order by 2,3

   select *,(rollingcount/population)*100
   from #percentofpopulation 
      drop table if exists #percentofpopulation
   CREATE TABLE #percentofpopulation 
(
    continent varchar(255),
    location varchar(255),
    date datetime,
    population numeric,
    new_vaccinations numeric,
    rollingcount numeric
)

INSERT INTO #percentofpopulation 
SELECT 
    ded.continent, 
    ded.location, 
    ded.date, 
    ded.population, 
    vac.new_vaccinations, 
    SUM(vac.new_vaccinations) OVER (PARTITION BY ded.location ORDER BY ded.location, ded.date) AS rollingcount
FROM 
    CovidDeaths AS ded 
JOIN 
    CovidVaccinations AS vac ON ded.location = vac.location AND ded.date = vac.date

SELECT *,     (rollingcount / population) * 100 
FROM #percentofpopulation

create view percentofpopulation as
select ded.continent, ded.location, ded.date, ded.population, vac.new_vaccinations, sum(cast(vac.new_vaccinations as int)) over (partition by ded.location order by ded.location, ded.date) as rollingcount
   from CovidDeaths as ded
  join CovidVaccinations as vac
  on ded.location=vac.location
   and ded.date=vac.date
   where ded.continent is not null
   --order by 2,3

   select * from percentofpopulation
