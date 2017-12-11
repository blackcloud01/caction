from IPython import embed

import SP_DB_CONNECT as spdbc
import TL_DB_CONNECT as tldbc
import datetime
import argparse
import pytz


# ---------------------------------------
# Convert datetime to a different timezone
# ---------------------------------------
def convert_datetime_to_timezone( date, from_timezone, to_timezone):
    """
    timezone - should be one of the timezones under pytz.all_timezones
    """
    target_timezone = pytz.timezone(to_timezone)
    orig_timezone   = pytz.timezone( from_timezone)
    cdate = orig_timezone.localize( date)
    target_zdate = cdate.astimezone( target_timezone)
    return target_zdate

# convert date string to datetime 
def date2dttm( date_list, inlist=True, dtstr_fmt="%Y%m%d"):
    import datetime as dttm
    if inlist:
        if "%H" in dtstr_fmt:
            return [dttm.datetime.strptime(x, dtstr_fmt) for x in date_list]
        else:
            return [dttm.datetime.strptime(x, dtstr_fmt).date() for x in date_list]
    else:
        x = date_list
        if "%H" in dtstr_fmt:
            return dttm.datetime.strptime(x, dtstr_fmt)
        else:
            return dttm.datetime.strptime(x, dtstr_fmt).date()

def get_cid_from_tiid(tiid):
    """
    A company id(cid) could have many tradingitemid mapped to it - one for each exchange.
    """
    qstr = """select relatedCompanyId from ciqSymbol where objectId = %s and symbolEndDate is null"""%tiid
    result = spdbc.dbquery( qstr)
    if result:
        return result[0][0]
    else:
        return None


def get_dates_for_event_query(look_ahead_days):
    # get previous business day's close datetime 
    # TO DO previous day could be a holiday
    today = datetime.datetime.today()

    # Events dates are stored as UTC timezone dates. So we convert our dates to UTC
    today_utc = convert_datetime_to_timezone( today, 'US/Central', 'UTC')
    end_date_utc = today_utc + datetime.timedelta(days = look_ahead_days)

    today = today.date()
    # get previous business date
    prev_date = today - datetime.timedelta( days = 7 - today.weekday() if today.weekday()>3 else 1)
    # get previous business datetime with close time 15:00
    prev_dt = datetime.datetime( prev_date.year, prev_date.month, prev_date.day, 15, 0, 0)
    # convert prev close datetime to UTC timezone
    prev_close_utc = convert_datetime_to_timezone( prev_dt, 'US/Central', 'UTC')
    
    return prev_close_utc, end_date_utc


def get_all_upcoming_events( tiid, look_ahead_days = 0):
    """
    Get upcoming events of all types for a given company id for day(s) ahead. By default we only look
    for events on current day.

    For help with table structure/join/meaning contact Dashiell, Douglas <douglas.dashiell@spglobal.com>

    """
    
    cid = get_cid_from_tiid(tiid)

    if cid == None:
        print "CompanyID mapping to TradingItemID not found"
        raise
    else:
        pass

    prev_close_utc, end_date_utc = get_dates_for_event_query( look_ahead_days)

    qstr = """
    declare @cid as int = %s
    declare @fromdate as datetime = '%s'
    declare @todate as datetime = '%s'
    """%( cid, prev_close_utc.strftime("%Y%m%d %H:%M:%S"), end_date_utc.strftime("%Y%m%d %H:%M:%S"))

    qstr +="""

    select 

    et.objectId as CompanyID,
    c.companyName, 
    e.mostImportantDateUTC as EarningsDate,
    e.headline,
    et.keyDevEventTypeId,
    ett.keyDevEventTypeName

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
    """
    #print qstr
    result = spdbc.dbquery( qstr)
    #print result
    if result:
        header = ("companyid", "companyname", "eventdate", "headline", "eventid", "eventname")
        return [header]+result
    else:
        return None




def get_upcoming_events_by_type( tiid, event_type_id, look_ahead_days = 0):
    """
    Get upcoming events of a specific type for a given company id for day(s) ahead. By default we only look
    for events on current day.

    Following are specific

    Events and  Event Type ID

    48 Earnings Calls
    49 Guidance/Update Calls
    50 Shareholder/Analyst Calls
    51 Company Conference Presentations
    52 M&A Calls
    55 Earnings Release Date
    62 Annual General Meeting
    78 Board Meeting
    97 Special/Extraordinary Shareholders Meeting
    139 Sales/Trading Statement Calls
    140 Sales/Trading Statement Release Date
    144 Estimated Earnings Release Date (S&P Global Derived)
    149 Conferences
    192 Analyst/Investor Day
    194 Special Calls
    219 Operating Results Release Date
    220 Interim Management Statement Release Date
    221 Operating Results Calls
    222 Interim Management Statement Calls
    223 Fixed Income Calls


    For help with table structure/join/meaning contact Dashiell, Douglas <douglas.dashiell@spglobal.com>

    """

    cid = get_cid_from_tiid(tiid)
    if cid == None:
        print "CompanyID mapping to TradingItemID not found"
        raise
    else:
        pass

    prev_close_utc, end_date_utc = get_dates_for_event_query( look_ahead_days)

    qstr = """
    declare @cid as int = %s
    declare @event_type_id as int = %s
    declare @fromdate as datetime = '%s'
    declare @todate as datetime = '%s'
    
    """%( cid, event_type_id, prev_close_utc.strftime("%Y%m%d %H:%M:%S"), end_date_utc.strftime("%Y%m%d %H:%M:%S"))


    qstr +="""select et.objectId as CompanyID,c.companyName, e.mostImportantDateUTC as EarningsDate,
    e.headline,et.keyDevEventTypeId,ett.keyDevEventTypeName
    from xf_trial.dbo.ciqevent e
    join xf_trial.dbo.ciqEventToObjectToEventType et on et.keyDevId=e.keyDevId
    left join xf_trial.dbo.ciqEventERInfo er on e.keyDevId=er.keyDevId
    left join xf_trial.dbo.ciqEventMarketIndicatorType em on em.marketIndicatorTypeId=er.marketIndicatorTypeId
    join xf_trial.dbo.ciqCompany c on et.objectId = c.companyId
    join xf_trial.dbo.ciqEventType ett on et.keyDevEventTypeId = ett.keyDevEventTypeId
    where 1=1
    and et.keyDevEventTypeId in (@event_type_id)
    and et.objectId in (@cid)
    and e.mostImportantDateUTC between @fromdate and @todate
    """

    result = spdbc.dbquery( qstr)
    if result:
        return result[0]
    else:
        return None




def check_if_current_day_is_dvd_exdate(tiid):
    """
    Check if current day is a dividend exdate.

    For help with table structure/join/meaning contact Dashiell, Douglas <douglas.dashiell@spglobal.com>

    Comment from Doug:
    The Dividend information (historical and forward looking) was not included in the Events package as
    it would replicate an existing data set and this would not be as efficient from a storage prospective on the client side.

    """

    qstr = """declare @tiid as int = %s"""%( tiid)
    qstr +="""
    select case when exists(
    select exDate from ciqDividend where tradingItemId=@tiid
    and exDate between dateadd(day, -1, getdate()) and getdate()
    )
    then cast(1 as bit)
    else cast(0 as bit) end
    """

    result = spdbc.dbquery( qstr)

    if result:
        return result[0][0]
    else:
        return None

def check_if_current_day_is_split_exdate(tiid):
    """
    Check if current day is a split exdate.

    For help with table structure/join/meaning contact Dashiell, Douglas <douglas.dashiell@spglobal.com>

    Comment from Doug:
    The Dividend information (historical and forward looking) was not included in the Events package as
    it would replicate an existing data set and this would not be as efficient from a storage prospective on the client side.

    """

    qstr = """declare @tiid as int = %s"""%( tiid)
    qstr +="""
    select case when exists(
    select exDate from ciqSplit where tradingItemId=@tiid
    and exDate between dateadd(day, -1, getdate()) and getdate()
    )
    then cast(1 as bit)
    else cast(0 as bit) end
    """

    result = spdbc.dbquery( qstr)

    if result:
        return result[0][0]
    else:
        return None


def get_dividend_info_on_date_range( tiid, start_date, end_date):
    """
    Returns the dividend details for a stock(tiid) for a given date range

    For help with table structure/join/meaning contact Dashiell, Douglas <douglas.dashiell@spglobal.com>

    Comment from Doug:
    The Dividend information (historical and forward looking) was not included in the Events package as
    it would replicate an existing data set and this would not be as efficient from a storage prospective on the client side.
    
    """

    qstr = """
    declare @tiid as int = %s
    declare @start_date as date = '%s'
    declare @end_date as date = '%s'
    """%( tiid, start_date, end_date)
    
    qstr +="""
    select 
    cd.exDate, 
    cd.payDate, 
    cd.recordDate, 
    cd.announcedDate, 
    cast(cd.divAmount as float), 
    cast(cd.grossAmount as float), 
    cc.ISOCode 
    from ciqDividend cd
    join ciqCurrency cc on cc.currencyId = cd.currencyId
    where 
    cd.tradingItemId=@tiid and 
    cd.exDate >= @start_date and 
    cd.exDate <= @end_date
    order by  cd.exDate
    """
    
    result = spdbc.dbquery( qstr)
    header = ['exDate', 'payDate', 'recordDate', 'announcedDate', 'divAmount', 'grossAmount', 'ISOCode']
    if result:
        return [header] + result
    else:
        return None
    
