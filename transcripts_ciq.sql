declare @mydate as datetime = '20070117'
declare @gvkey as varchar(6) = '001690'
select * from ciqGvkeyIID where gvkey=@gvkey order by symbolStartDate

select top 10 * from ciqGvkeyIID where gvkey = @gvkey order by symbolStartDate

select top 10 * from xf.dbo.ciqCompany where companyId in( 257948)

select top 10 * from ciqSymbol where objectId in (8795473)

select * from ciqSymbol cs
join ciqExchange ce on ce.exchangeId = cs.exchangeId
where cs.symbolValue ='IMB'
and cs.activeFlag = 1
order by cs.exchangeId

select * from ciqExch>ange where exchangeId = 3
order by exchangeName


select relatedCompanyId, * from ciqGvkeyIID where gvkey = @gvkey and
(
( symbolStartDate is not null  and symbolEndDate is not null and symbolStartDate <= @mydate and symbolEndDate >= @mydate)
or
( symbolStartDate is null  and symbolEndDate is not null and symbolEndDate >= @mydate)
or
( symbolStartDate is not null  and symbolEndDate is null and symbolStartDate <= @mydate)
or
( symbolStartDate is null  and symbolEndDate is null)
)




select top 10 * from ciqGvkeyIID where  symbolStartDate >= '20070101' and symbolEndDate <= '20170101' and gvkey = '001690'

select top 10 * from xf_trial.dbo.ciqEvent
select top 10 * from xf_trial.dbo.ciqEvent where keyDevId=268230709
select  * from xf_trial.dbo.ciqEventType


select top 10 * from xf_trial.dbo.ciqTranscript

select top 10 * from xf_trial.dbo.ciqEventToObjectToEventType

--select  * from c
select  * from xf_trial.dbo.ciqEventType
select  * from xf_trial.dbo.ciqEventToObjectToEventType 
where 
objectId='24937' and -- AAPL Company Id
keyDevEventTypeId = 55 --earnings call 48

select  * from xf_trial.dbo.ciqEventToObjectToEventType where keyDevEventTypeId=49

select * from xf_trial.dbo.ciqTranscript where keyDevId = 268230709
select * from xf_trial.dbo.ciqTranscript where keyDevId = 535739949

select * from xf_trial.dbo.ciqTranscript ct
join xf_trial.dbo.ciqTranscriptCollectionType cty on cty.transcriptCollectionTypeId = ct.transcriptCollectionTypeId
join xf_trial.dbo.ciqTranscriptPresentationType ctp on ctp.transcriptPresentationTypeId = ct.transcriptPresentationTypeId
 where ct.keyDevId = 5107597
 order by ct.transcriptCreationDateUTC

select * from xf_trial.dbo.ciqEvent where keyDevId = 289919480
select * from xf_trial.dbo.ciqEvent where keyDevId =535739949

select * from xf_trial.dbo.ciqTranscriptComponent tc
join xf_trial.dbo.ciqTranscriptComponentType tci on tci.transcriptComponentTypeId = tc.transcriptComponentTypeId
join xf_trial.dbo.ciqTranscriptPerson tcp on tcp.transcriptPersonId = tc.transcriptPersonId
join xf_trial.dbo.ciqTranscriptSpeakerType tcs on tcs.speakerTypeId = tcp.speakerTypeId
 where tc.transcriptId = 552614
 order by tc.componentOrder


select 
tc.transcriptComponentTypeId, 
tcp.speakerTypeId,
tc.componentText
from xf_trial.dbo.ciqTranscriptComponent tc
join xf_trial.dbo.ciqTranscriptComponentType tci on tci.transcriptComponentTypeId = tc.transcriptComponentTypeId
join xf_trial.dbo.ciqTranscriptPerson tcp on tcp.transcriptPersonId = tc.transcriptPersonId
join xf_trial.dbo.ciqTranscriptSpeakerType tcs on tcs.speakerTypeId = tcp.speakerTypeId
 where tc.transcriptId = 552614
 order by tc.componentOrder

--select * from xf_trial.dbo.ciqEvent where keyDevId = 2711399

select * from xf_trial.dbo.ciqTranscriptCollectionType
select * from xf_trial.dbo.ciqTranscriptComponentType
select * from xf_trial.dbo.ciqTranscriptSpeakerType
select * from xf_trial.dbo.ciqTranscriptPresentationType
select top 10 * from xf_trial.dbo.ciqTranscriptPerson
select top 10 * from xf_trial.dbo.ciqTranscriptSpeakerType

-- Get event type for every transcript ID
select  e.keyDevEventTypeId, et.keyDevEventTypeName, t.transcriptId, * from xf_trial.dbo.ciqTranscript t 
join xf_trial.dbo.ciqEventToObjectToEventType e on e.keyDevId = t.keyDevId
join xf_trial.dbo.ciqEventType et on e.keyDevEventTypeId = et.keyDevEventTypeId
where e.keyDevEventTypeId != 48

-- total no of transcripts by category
select  et.keyDevEventTypeId, et.keyDevEventTypeName, count(t.transcriptId) as ct from xf_trial.dbo.ciqTranscript t 
join xf_trial.dbo.ciqEventToObjectToEventType e on e.keyDevId = t.keyDevId
join xf_trial.dbo.ciqEventType et on e.keyDevEventTypeId = et.keyDevEventTypeId
--where e.keyDevEventTypeId != 48
group by et.keyDevEventTypeId , et.keyDevEventTypeName
order by ct

-- total no of transcripts by event type and company
select  et.keyDevEventTypeName, count( distinct e.objectId) as ct from xf_trial.dbo.ciqTranscript t 
join xf_trial.dbo.ciqEventToObjectToEventType e on e.keyDevId = t.keyDevId
join xf_trial.dbo.ciqEventType et on e.keyDevEventTypeId = et.keyDevEventTypeId
group by et.keyDevEventTypeName
order by ct

-- total no of transcripts by event type and company and year
select  
et.keyDevEventTypeName, 
cast(YEAR( t.transcriptCreationDateUTC) as VARCHAR(4)) as yr, 
count( distinct e.objectId) as ct 
from xf_trial.dbo.ciqTranscript t 
join xf_trial.dbo.ciqEventToObjectToEventType e on e.keyDevId = t.keyDevId
join xf_trial.dbo.ciqEventType et on e.keyDevEventTypeId = et.keyDevEventTypeId
group by et.keyDevEventTypeName, cast(YEAR( t.transcriptCreationDateUTC) as VARCHAR(4))
order by yr, et.keyDevEventTypeName, ct

-- total no of transcripts by event type and year
select  
et.keyDevEventTypeName, 
cast(YEAR( t.transcriptCreationDateUTC) as VARCHAR(4)) as yr, 
count(t.transcriptId) as ct
from xf_trial.dbo.ciqTranscript t 
join xf_trial.dbo.ciqEventToObjectToEventType e on e.keyDevId = t.keyDevId
join xf_trial.dbo.ciqEventType et on e.keyDevEventTypeId = et.keyDevEventTypeId
group by et.keyDevEventTypeName, cast(YEAR( t.transcriptCreationDateUTC) as VARCHAR(4))
order by yr, et.keyDevEventTypeName, ct


-- fixed income transcripts
select  et.keyDevEventTypeId, et.keyDevEventTypeName, e.*, t.* from xf_trial.dbo.ciqTranscript t 
join xf_trial.dbo.ciqEventToObjectToEventType e on e.keyDevId = t.keyDevId
join xf_trial.dbo.ciqEventType et on e.keyDevEventTypeId = et.keyDevEventTypeId
where e.keyDevEventTypeId = 223


select * from xf_trial.dbo.ciqTranscriptComponent where transcriptId = 662597

-- unique companies with fixed income transcripts
select  distinct e.objectId, c.companyName, c.simpleIndustryId from xf_trial.dbo.ciqEventToObjectToEventType e 
join ciqCompany c on c.companyId=e.objectId
where e.keyDevEventTypeId = 223
order by simpleIndustryId

select top 10 * from ciqCompany where 'SPDR' in companyName


-- get all AAPL events with their dates
select  * from xf_trial.dbo.ciqEventToObjectToEventType et
join xf_trial.dbo.ciqEvent e on e.keyDevId = et.keyDevId
where 
et.objectId='24937' and -- AAPL Company Id 24937
et.keyDevEventTypeId = 55 -- earnings call 48
--and e.mostImportantDateUTC >= '20070901'
order by e.mostImportantDateUTC

-----------------------------------------------------------------------
-- Get keydevs on a date               START                         --
-----------------------------------------------------------------------
declare @modeldate datetime = '20170912 11:00:00'
declare @prevmodeldate datetime = '20170911'
declare @cid varchar(6) = '27970'
--Display During and After-Market Earnings Releases for a specific date
select et.objectId as CompanyID, et.keyDevEventTypeId, e.keyDevId, e.mostImportantDateUTC as EarningsDate, e.headline
from xf_trial.dbo.ciqevent e
join xf_trial.dbo.ciqEventToObjectToEventType et on et.keyDevId=e.keyDevId
--join xf_trial.dbo.ciqEventERInfo er on e.keyDevId=er.keyDevId
-- join xf_trial.dbo.ciqEventMarketIndicatorType em on em.marketIndicatorTypeId=er.marketIndicatorTypeId
where 
et.objectId = @cid and
--em.marketIndicatorTypeId in ('2', '3') --- During- and After Market
e.mostImportantDateUTC >= @prevmodeldate and e.mostImportantDateUTC < @modeldate
and et.keyDevEventTypeId = 48

UNION

--Display pre and during market Earnings Releases for a specific date
select et.objectId as CompanyID, et.keyDevEventTypeId, e.keyDevId, e.mostImportantDateUTC as EarningsDate, e.headline
from xf_trial.dbo.ciqevent e
join xf_trial.dbo.ciqEventToObjectToEventType et on et.keyDevId=e.keyDevId
--join xf_trial.dbo.ciqEventERInfo er on e.keyDevId=er.keyDevId
--join xf_trial.dbo.ciqEventMarketIndicatorType em on em.marketIndicatorTypeId=er.marketIndicatorTypeId
--where 1=1 and 
where 
et.objectId = @cid and
--em.marketIndicatorTypeId in ('1') --- pre-market
e.mostImportantDateUTC = @modeldate
and et.keyDevEventTypeId = 48
order by EarningsDate

-----------------------------------------------------------------------
-- Get keydevs on a date               END                           --
-----------------------------------------------------------------------


select top 10 * from xf_trial.dbo.ciqEventMarketIndicatorType
select top 10 * from xf_trial.dbo.ciqEventERInfo

select * from xf_trial.dbo.ciqEvent where keyDevId =535739949
select * from xf_trial.dbo.ciqTranscript where keyDevId =264135054


SELECT t.keyDevId, t.transcriptId, t.transcriptCreationDateUTC, e.headline, comp.companyId,

comp.companyName

FROM xf_trial.dbo.ciqTranscript t (NOLOCK)

JOIN xf_trial.dbo.ciqEvent e (NOLOCK) ON e.keyDevId = t.keyDevId

JOIN xf_trial.dbo.ciqEventToObjectToEventType ete (NOLOCK) ON ete.keyDevId = t.keyDevId

JOIN ciqCompany comp (NOLOCK) ON comp.companyId = ete.objectId

WHERE e.keyDevId = 535739902

order by 3 desc

declare @modeldate datetime = '20170912 11:00:00'
declare @prevmodeldate datetime = '20170911'
declare @cid varchar(6) = '27970'
SELECT t.keyDevId,--  t.transcriptCreationDateUTC, --t.transcriptId, e.headline, comp.companyId, comp.companyName, 
min(t.transcriptCreationDateUTC), max( t.transcriptCreationDateUTC), count( t.transcriptId)
FROM xf_trial.dbo.ciqTranscript t (NOLOCK)
--JOIN xf_trial.dbo.ciqEvent e (NOLOCK) ON e.keyDevId = t.keyDevId
JOIN xf_trial.dbo.ciqEventToObjectToEventType ete (NOLOCK) ON ete.keyDevId = t.keyDevId
JOIN ciqCompany comp (NOLOCK) ON comp.companyId = ete.objectId
WHERE comp.companyId = @cid
group by t.keyDevId
order by 3


select * from xf_trial.dbo.ciqTranscript where keyDevId = 139632814 order by transcriptCreationDateUTC
select * from xf_trial.dbo.ciqTranscriptCollectionType


declare @modeldate datetime = '20170912 11:00:00'
declare @prevmodeldate datetime = '20170911 11:00:00'
declare @cid varchar(6) = '27970'
SELECT t.keyDevId,  mxt.dt--, t.transcriptId--, e.headline, comp.companyId, comp.companyName
--min(t.transcriptCreationDateUTC), max( t.transcriptCreationDateUTC), count( t.transcriptId)
FROM xf_trial.dbo.ciqTranscript t (NOLOCK)
JOIN xf_trial.dbo.ciqEvent e (NOLOCK) ON e.keyDevId = t.keyDevId
JOIN xf_trial.dbo.ciqEventToObjectToEventType ete (NOLOCK) ON ete.keyDevId = t.keyDevId
JOIN ciqCompany comp (NOLOCK) ON comp.companyId = ete.objectId
JOIN (select t2.keyDevId as kid, max( t2.transcriptCreationDateUTC) as dt  from xf_trial.dbo.ciqTranscript t2 group by t2.keyDevId) mxt on mxt.kid = t.keyDevId

WHERE comp.companyId = @cid and
t.transcriptCreationDateUTC <= @modeldate and
t.transcriptCreationDateUTC >= @prevmodeldate

group by t.keyDevId, t.transcriptId


-- All transcripts on a single name on a single day pre open
declare @modeldate datetime = '20170912 11:00:00'
declare @prevmodeldate datetime = '20170911 11:00:00'
declare @cid varchar(6) = '27970'
SELECT t.keyDevId, t.transcriptCreationDateUTC, t.transcriptId 
FROM xf_trial.dbo.ciqTranscript t (NOLOCK)
--JOIN xf_trial.dbo.ciqEvent e (NOLOCK) ON e.keyDevId = t.keyDevId
JOIN xf_trial.dbo.ciqEventToObjectToEventType ete (NOLOCK) ON ete.keyDevId = t.keyDevId
JOIN ciqCompany comp (NOLOCK) ON comp.companyId = ete.objectId
WHERE comp.companyId = @cid and
t.transcriptCreationDateUTC <= @modeldate and
t.transcriptCreationDateUTC >= @prevmodeldate

-- All transcripts on a single name
declare @cid varchar(6) = '27970'
SELECT t.keyDevId, t.transcriptCreationDateUTC, t.transcriptId 
FROM xf_trial.dbo.ciqTranscript t (NOLOCK)
--JOIN xf_trial.dbo.ciqEvent e (NOLOCK) ON e.keyDevId = t.keyDevId
JOIN xf_trial.dbo.ciqEventToObjectToEventType ete (NOLOCK) ON ete.keyDevId = t.keyDevId
JOIN ciqCompany comp (NOLOCK) ON comp.companyId = ete.objectId
WHERE comp.companyId = @cid 
order by t.transcriptCreationDateUTC

-- Earliest transcript on a single name for every keydev
--declare @cid varchar(6) = '27970'
SELECT t.keyDevId, t.transcriptCreationDateUTC, t.transcriptId 
FROM xf_trial.dbo.ciqTranscript t (NOLOCK)
--JOIN xf_trial.dbo.ciqEvent e (NOLOCK) ON e.keyDevId = t.keyDevId
JOIN xf_trial.dbo.ciqEventToObjectToEventType ete (NOLOCK) ON ete.keyDevId = t.keyDevId
JOIN ciqCompany comp (NOLOCK) ON comp.companyId = ete.objectId
WHERE comp.companyId = @cid 
and
t.transcriptCreationDateUTC in (select min( t2.transcriptCreationDateUTC) from xf_trial.dbo.ciqTranscript t2 where t2.keyDevId = t.keyDevId)
order by t.transcriptCreationDateUTC


select distinct feet.keyDevEventTypeId, feet.keyDevEventTypeName
from xf_trial.dbo.ciqKeyDevToObjectToEventType feoet (nolock)
join xf_trial.dbo.ciqKeyDevEventType feet (nolock) on feoet.keyDevEventTypeId = feet.keyDevEventTypeId
order by feet.keyDevEventTypeId asc

-- US companies with earnings releases in the next two weeks
select
feo.keyDevId
,fet.keyDevEventTypeName
,co.companyName
,fert.keyDevToObjectRoleTypeName 'Role Type'
,convert(varchar, fe.announcedDate, 101) 'Announced Date'
,convert(varchar, fe.mostImportantDateUTC, 101) 'Earnings Release'
,fe.headline
from xf_trial.dbo.ciqFutEventToObjToEventType feo
join xf_trial.dbo.ciqFutureEvent fe on fe.keyDevId = feo.keyDevId
join xf_trial.dbo.ciqCompany co on co.companyId = feo.objectId
join xf_trial.dbo.ciqFutureEventObjectRoleType fert on fert.keyDevToObjectRoleTypeId = feo.keyDevToObjectRoleTypeId
join xf_trial.dbo.ciqFutureEventEventType fet on fet.keyDevEventTypeId = feo.keyDevEventTypeId
where
--look for all keyDevIds associated with Expected Earnings Release Date
--to ensure that we get all related companies and event types
feo.keyDevId in (select a.keyDevId from xf_trial.dbo.ciqFutEventToObjToEventType a where a.keyDevEventTypeId = 55)
--announcement will happen within next week
and fe.mostImportantDateUTC between getdate() and dateadd(week, 1, getdate())
--U.S. Companies
and co.countryId = 213
order by fe.mostImportantDateUTC asc, feo.keyDevId



--Display After-Market Earnings Releases for a specific date
select et.objectId as CompanyID, e.keyDevId, e.mostImportantDateUTC as EarningsDate, em.marketIndicatorTypeName, e.headline, em.*
from xf_trial.dbo.ciqevent e
join xf_trial.dbo.ciqEventToObjectToEventType et on et.keyDevId=e.keyDevId
join xf_trial.dbo.ciqEventERInfo er on e.keyDevId=er.keyDevId
join xf_trial.dbo.ciqEventMarketIndicatorType em on em.marketIndicatorTypeId=er.marketIndicatorTypeId
where 1=1
--and em.marketIndicatorTypeId='3' --- After Market
and e.mostImportantDateUTC = '9/21/2017'



--Display a list of Earnings Calls for HTC Corporation with additional detail
SELECT ev.keydevid,
ev.mostImportantDateUTC,
ety.keydeveventtypename,
eb.calldescription,
liveURL,
replayURL,
eb.fiscalyear,
el.languagename,
ed.hostfirstname1,
ed.hostlastname1
FROM xf_trial.dbo.ciqEvent ev
JOIN xf_trial.dbo.ciqeventcallbasicinfo eb (nolock) on eb.keydevid = ev.keydevid
JOIN xf_trial.dbo.ciqeventcalladditionalinfo ed (nolock) on ed.keydevid = ev.keydevid
JOIN xf_trial.dbo.ciqeventlanguage el (nolock) on el.languageid = eb.languageid
JOIN xf_trial.dbo.ciqeventtoobjecttoeventtype eo (nolock) on eo.keydevid = ev.keydevid
--AND eo.keydeveventtypeid = 48 -- Earnings Calls
AND eo.keydeveventtypeid = 55 -- Earnings release
JOIN xf_trial.dbo.ciqeventobjectroletype rt (nolock) on rt.keydevtoobjectroletypeid = eo.keydevtoobjectroletypeid
JOIN xf_trial.dbo.ciqeventtype ety (nolock) on ety.keydeveventtypeid = eo.keydeveventtypeid
WHERE 1=1
AND eo.objectid = 27970 -- HTC Corporation
ORDER BY ev.mostImportantDateUTC DESC


select top 10 * from xf_trial.dbo.ciqEvent where keyDevId = 314041596
select top 10 * from xf_trial.dbo.ciqEventSource where keyDevId = 314041596
select top 10 * from xf_trial.dbo.ciqEventToObjectToEventType   where keyDevId = 314041596

select top 10 * from xf_trial.dbo.ciqEventERInfo where quarterEndDate > '20161231'
select top 10 * from xf_trial.dbo.ciqEventERInfo where keyDevId = 314041596


-- get all earnings release before a specific date
declare @mdate datetime = '20170912'
--declare @cid varchar(6) = '313055' -- WMT
select ce.keyDevId, ce.mostImportantDateUTC from 
xf_trial.dbo.ciqEventToObjectToEventType ceo
join xf_trial.dbo.ciqEvent ce on ce.keyDevId = ceo.keyDevId
 where ceo.keyDevEventTypeId=55 and ceo.objectId = @cid and
 ce.mostImportantDateUTC <= @mdate
 order by ce.mostImportantDateUTC



 

 select et.objectId as CompanyID, e.keyDevId, e.mostImportantDateUTC as EarningsDate, em.marketIndicatorTypeName, e.headline
from xf_trial.dbo.ciqevent e
join xf_trial.dbo.ciqEventToObjectToEventType et on et.keyDevId=e.keyDevId
join xf_trial.dbo.ciqEventERInfo er on e.keyDevId=er.keyDevId
join xf_trial.dbo.ciqEventMarketIndicatorType em on em.marketIndicatorTypeId=er.marketIndicatorTypeId
where 1=1
--and em.marketIndicatorTypeId='3' --- After Market
and e.mostImportantDateUTC = '10/23/2017'



select et.objectId as CompanyID,c.companyName, e.keyDevId, e.mostImportantDateUTC as EarningsDate,
em.marketIndicatorTypeName, e.headline,et.keyDevEventTypeId,ett.keyDevEventTypeName,er.fiscalQuarter,er.fiscalYear
from xf_trial.dbo.ciqevent e
join xf_trial.dbo.ciqEventToObjectToEventType et on et.keyDevId=e.keyDevId
left join xf_trial.dbo.ciqEventERInfo er on e.keyDevId=er.keyDevId
left join xf_trial.dbo.ciqEventMarketIndicatorType em on em.marketIndicatorTypeId=er.marketIndicatorTypeId
join xf_trial.dbo.ciqCompany c on et.objectId = c.companyId
join xf_trial.dbo.ciqEventType ett on et.keyDevEventTypeId = ett.keyDevEventTypeId
where 1=1
--and et.keyDevEventTypeId in (48,55,144)
and e.mostImportantDateUTC between getdate() and dateadd(day, 1, getdate())
--and et.objectId in (24937)
order by e.mostImportantDateUTC 



declare @cid as int = 24937
    declare @event_type_id as int = 144
    select et.objectId as CompanyID,c.companyName, e.mostImportantDateUTC as EarningsDate,
    e.headline,et.keyDevEventTypeId,ett.keyDevEventTypeName
    from xf_trial.dbo.ciqevent e
    join xf_trial.dbo.ciqEventToObjectToEventType et on et.keyDevId=e.keyDevId
    left join xf_trial.dbo.ciqEventERInfo er on e.keyDevId=er.keyDevId
    left join xf_trial.dbo.ciqEventMarketIndicatorType em on em.marketIndicatorTypeId=er.marketIndicatorTypeId
    join xf_trial.dbo.ciqCompany c on et.objectId = c.companyId
    join xf_trial.dbo.ciqEventType ett on et.keyDevEventTypeId = ett.keyDevEventTypeId
    where 1=1
    and et.keyDevEventTypeId in (@event_type_id)
--    and et.objectId in (@cid)
    and e.mostImportantDateUTC = getdate()

declare @cdate as date = getDate()
select @cdate
select dateadd(hour, 15, dateadd(day, -1, @cdate))
--declare @cdate AS datetime = FLOOR(CAST(GETDATE() AS float))
declare @cdate AS datetime 
set @cdate = GETDATE() at TIME ZONE 'UTC' --as float
select @cdate


declare @udate as datetimeoffset(6) = getdate()
set @udate =  TODATETIMEOFFSET(@udate, 360)
select @udate, getdate(), TODATETIMEOFFSET(@udate, 360)

select dateadd(day, 1, getdate()) at time zone 'UTC';

select * from xf_trial.dbo.ciqEventType

SELECT c.companyName
,d.tradingItemId
,d.dividendId
,d.exDate
,d.payDate
,d.recordDate
,d.divAmount
,d.divFreqTypeId
,ft.divFreqTypeName
FROM ciqDividend d
join ciqTradingItem ti on d.tradingItemId=ti.tradingItemId
join ciqSecurity s on ti.securityId=s.securityId
join ciqCompany c on s.companyId=c.companyId
join ciqDividendFrequencyType ft on d.divFreqTypeId=ft.divFreqTypeId
WHERE d.exDate between getdate() and dateadd(day, 2, getdate())

select * from ciqSymbol cs
join ciqExchange ce on ce.exchangeId = cs.exchangeId
--where cs.symbolValue ='GE'
where cs.symbolValue like 'PATK%'
and cs.activeFlag = 1
order by cs.exchangeId

select * from ciqSymbol cs
join ciqExchange ce on ce.exchangeId = cs.exchangeId
where cs.symbolValue ='DLPH'
and cs.activeFlag = 1
and ce.currencyId = 160
and ce.countryId = 213
order by cs.exchangeId

SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('ciqSymbol') 


select * from ciqSymbol where exchangeId=87

select * from ciqExchange order by exchangeName

select * from ciqSymbol where
relatedCompanyId = 24937 and
exchangeId = 458 and
activeFlag = 1 and
symbolEndDate is null

select top 10 * from ciqTradingItem where tickerSymbol='AAPL' and primaryFlag=1

select * from ciqExchange where countryId=213 and importancelevel is not null order by importancelevel
select * from ciqCurrency order by currencyName
select * from ciqCountryGeo order by country

declare @cid as int = 33493
declare @tiid as int = 2641396>
    SELECT c.companyName
    ,d.tradingItemId
    ,d.dividendId
    ,d.exDate
    ,d.payDate
    ,d.recordDate
    ,d.divAmount
    ,d.divFreqTypeId
    ,ft.divFreqTypeName
	,s.securityId
    FROM ciqDividend d
    join ciqTradingItem ti on d.tradingItemId=ti.tradingItemId
    join ciqSecurity s on ti.securityId=s.securityId
    join ciqCompany c on s.companyId=c.companyId
    join ciqDividendFrequencyType ft on d.divFreqTypeId=ft.divFreqTypeId
    WHERE 1=1 and
    s.companyId in (@cid) and
	d.tradingItemId in (@tiid) and
	d.exDate between dateadd(day, -1, getdate()) and dateadd(day, 1, getdate())
--    and d.exDate between dateadd(day, -200, getdate()) and getDate()>

select top 10 * from ciqTradingItem where tradingItemId=2641396

declare @cdate datetime = getDate()
select 
case exDate = @cdate then 'TRUE' else 'FALSE' end
from ciqDividend
where
-- tradingItemId=2641396 and
exDate between dateadd(day, -1, getdate()) and getdate()
order by exDate

select case when exists(
select exDate from ciqDividend where tradingItemId=2641396 and
exDate between dateadd(day, -1, getdate()) and getdate()
)
then cast(1 as BIT)
else cast(0 as BIT) end




select dateadd(day, -1, getdate()) , getdate()


declare @cdate AS datetime = FLOOR(CAST(GETDATE() AS float))
declare @cid as int = 33493
declare @tiid as int = 2641396
    SELECT c.companyName
    ,d.tradingItemId
    ,d.dividendId
    ,d.exDate
    ,d.payDate
    ,d.recordDate
    ,d.divAmount
    ,d.divFreqTypeId
    ,ft.divFreqTypeName
	,s.securityId
    FROM ciqDividend d
    join ciqTradingItem ti on d.tradingItemId=ti.tradingItemId
    join ciqSecurity s on ti.securityId=s.securityId
    join ciqCompany c on s.companyId=c.companyId
    join ciqDividendFrequencyType ft on d.divFreqTypeId=ft.divFreqTypeId
    WHERE 1=1 and
    s.companyId in (@cid) and
	d.tradingItemId in (@tiid) and
	d.exDate between @cdate and dateadd(day, 1, @cdate)


declare @cdate AS datetime = FLOOR(CAST(GETDATE() AS float))
declare @cid as int = 59502420
    declare @event_type_id as int = 58
    select et.objectId as CompanyID,c.companyName, e.mostImportantDateUTC as EarningsDate,
    e.headline,et.keyDevEventTypeId,ett.keyDevEventTypeName
    from xf_trial.dbo.ciqevent e
    join xf_trial.dbo.ciqEventToObjectToEventType et on et.keyDevId=e.keyDevId
    left join xf_trial.dbo.ciqEventERInfo er on e.keyDevId=er.keyDevId
    left join xf_trial.dbo.ciqEventMarketIndicatorType em on em.marketIndicatorTypeId=er.marketIndicatorTypeId
    join xf_trial.dbo.ciqCompany c on et.objectId = c.companyId
    join xf_trial.dbo.ciqEventType ett on et.keyDevEventTypeId = ett.keyDevEventTypeId
    where 1=1
    --and et.keyDevEventTypeId in (@event_type_id)
    and et.objectId in (@cid)
    --and e.mostImportantDateUTC >= getDate()
	order by EarningsDate

	--between @cdate and dateadd(day, 1, @cdate)
--    and e.mostImportantDateUTC between dateadd(day, -1, @cdate) and dateadd(day, 20, @cdate)

declare @cdate AS datetime = FLOOR(CAST(GETDATE() AS float))
declare @cid as int = 59502420
declare @event_type_id as int = 58
select * from declare @cdate AS datetime = FLOOR(CAST(GETDATE() AS float))
declare @cid as int = 59502420
declare @event_type_id as int = 58



declare @cid as int = 141443332
    declare @fromdate as datetime = '20161127 21:00:00'
    declare @todate as datetime = '20180606 20:33:59'
    
    select et.objectId as CompanyID,c.companyName, e.mostImportantDateUTC as EarningsDate,
    e.headline,et.keyDevEventTypeId,ett.keyDevEventTypeName
    from xf_trial.dbo.ciqevent e
    join xf_trial.dbo.ciqEventToObjectToEventType et on et.keyDevId=e.keyDevId
    left join xf_trial.dbo.ciqEventERInfo er on e.keyDevId=er.keyDevId
    left join xf_trial.dbo.ciqEventMarketIndicatorType em on em.marketIndicatorTypeId=er.marketIndicatorTypeId
    join xf_trial.dbo.ciqCompany c on et.objectId = c.companyId
    join xf_trial.dbo.ciqEventType ett on et.keyDevEventTypeId = ett.keyDevEventTypeId
    where 1=1
    and et.objectId in (@cid)
    and e.mostImportantDateUTC between @fromdate and @todate
    order by e.mostImportantDateUTC



	select * from ciqDividend where tradingItemId=2601263 and exDate > '20170101'
	
	declare @tiid as int = 20172869
	select cd.exDate, cd.payDate, cd.recordDate, cd.announcedDate, cd.divAmount, cd.grossAmount, cc.ISOCode from ciqDividend cd
	join ciqCurrency cc on cc.currencyId = cd.currencyId
	where cd.tradingItemId=@tiid and exDate > '20160101'


	select  * from ciqSplit where  exDate = '20171128'

	select
	kdo.keyDevId
	,kde.keyDevEventTypeName
	,co.companyName
	,kdor.keyDevToObjectRoleTypeName 'Role Type'
	,convert(varchar, kd.announcedDate, 101) 'Announced Date'
	,convert(varchar, kd.enteredDate, 101) 'Entered Date'
	,kdo.keyDevEventTypeId
	,kde.keyDevEventTypeName
	,kd.headline
	,kd.situation
	from xf_trial.dbo.ciqKeyDevToObjectToEventType kdo
	join xf_trial.dbo.ciqKeyDev kd on kd.keyDevId = kdo.keyDevId
	join xf_trial.dbo.ciqKeyDevObjectRoleType kdor on kdor.keyDevToObjectRoleTypeId = kdo.keyDevToObjectRoleTypeId
	join xf_trial.dbo.ciqKeyDevEventType kde on kde.keyDevEventTypeId = kdo.keyDevEventTypeId
	--join to ciqCompany to grab company name
	join ciqCompany co on co.companyId = kdo.objectId
	where
	kd.keyDevId in (select a.keyDevId from xf_trial.dbo.ciqKeyDevToObjectToEventType a
	where
	a.keyDevEventTypeId in (52,80,81,65,82))
	and kdo.objectId in (
	7959935,30763,31158,24225822)
	--Announced Date is in the last 2 months
	and (convert(varchar, kd.announcedDate, 101))>=
	dateadd(month, -2, getdate())
	order by kdo.objectId,kd.announcedDate desc, kdo.keyDevId


