from IPython import embed

import SP_DB_CONNECT as spdbc
import TL_DB_CONNECT as tldbc
import argparse
import csv

"""
International exchanges

['GR', 'GY']             DEU     XETRA
['SM']                   ESP     Bolsa De Madrid
['SS']                   SWE     NASDAQ OMX Nordic
['LN']                   GBR     London Stock Exchange
['NA']                   NLD     NYSE Euronext Amsterdam
['FP']                   FRA     NYSE Euronext Paris
['VX', 'SW']             CHE     Swiss Exchange
['BB']                   BEL     NYSE Euronext Brussels
['IT']                   ITA     Borsa Italiana Electronic Share Market
['AV']                   AUT     Wiener Boerse AG
['NO']                   NOR     Oslo Bors ASA
['DC']                   DNK     OMX Nordic Exchange Copenhagen AS
['FH']                   FIN     NASDAQ OMX Helsinki Ltd


['HK']                   HKG     Hong Kong Exchanges and Clearing Ltd
['SP']                   SGP     Singapore Exchange
['KS']                   KOR     Korea Exchange Stock Market
['AU']                   AUS     ASX All Markets
['NZ']                   NZL     Auckland
['JP', 'JT']             JPN     Tokyo Stock Exchange


"""
def get_tiid_from_bb( bbticker, global_ = False):
    """
    Get the tradingitemid(tiid) from S&P CapitalIQ tables

    For help with table structure/join/meaning contact Dashiell, Douglas <douglas.dashiell@spglobal.com>

    """

    if global_ == False:
        ticker = bbticker.split(" ")[0]
        qstr = """
        select cs.objectId from ciqSymbol cs
        join ciqExchange ce on ce.exchangeId = cs.exchangeId
        where cs.symbolValue ='%s'
        and cs.activeFlag = 1 -- only currently active
        and ce.currencyId = 160 -- USD (use table ciqCurrency for id)
        and ce.countryId = 213 -- Country: USA (use table ciqCountryGeo for id)
        order by cs.exchangeId
        """%ticker
    else:
        # TO DO - global bloomberg ticker implementation
        pass
    #
    result = spdbc.dbquery( qstr)
    if result:
        return result[0][0]
    else:
        return None
    

