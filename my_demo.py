from IPython import embed

import SP_DB_CONNECT as spdbc
import TL_DB_CONNECT as tldbc
import argparse
import csv

import symbol
import corpaction



def read_csv( fname):
    data = []
    with open( fname, "r") as csvf:
        csvr = csv.reader( csvf)
        for row in csvr:
            data.append( row)
    #
    return data


def get_holdings_by_date(acct, date, global_=False):
    """
    Get Equity positions in tradelink sybase tables by acct on a date
    """
    qstr = """
    select
    '%s',
    pd.date,
    pd.symbol, 
    vid.vendor_name, 
    vid.vendor_symbol,
    pd.qty
    from trading..position_daily pd 
    join identifiers id on id.symbol = pd.symbol
    join vendor_identifiers vid on id.symbol = vid.symbol
    where pd.acct = '%s' and pd.instrument='E' 
    and pd.date='%s'
    and vid.vendor_name in ('CTA', 'BPIPE')
    """%(global_,acct, date)
    
    result = tldbc.dbquery(qstr)
    if result:
        header = ("global", "date", "tlsymbol", "vendor", "vendor_symbol", "qty")
        return [header] + result
    else:
        return None


def get_holdings_on_daterange(acct, start_date, end_date, global_=False):
    """
    Get Equity positions in tradelink sybase tables by acct for a date range
    """
    qstr = """
    select
    '%s',
    pd.date,
    pd.symbol, 
    vid.vendor_name, 
    vid.vendor_symbol,
    pd.qty
    from trading..position_daily pd 
    join identifiers id on id.symbol = pd.symbol
    join vendor_identifiers vid on id.symbol = vid.symbol
    where pd.acct = '%s' and pd.instrument='E' 
    and pd.date >= '%s'
    and pd.date <= '%s'
    and vid.vendor_name in ('CTA', 'BPIPE')
    order by pd.date
    """%(global_,acct, start_date, end_date)
    
    result = tldbc.dbquery(qstr)
    if result:
        header = ("global", "date", "tlsymbol", "vendor", "vendor_symbol", "qty")
        return [header] + result
    else:
        return None
    

def get_dvd_exdate_info_on_holdings( holdings):
    """
    Check to see if any holdings exist on a dvd ex-date

    The input 'holdings' is the output of get_holdings_by_date() function
    """
    header = list(holdings.pop(0)) + [ 'dvd_per_share', 'dvd_curr']
    dvd_info = [header]
    for global_, date, tlsymbol, vendor, vendor_symbol, qty in holdings:
        if global_ in ('False', 0, False, '0'):
            bbticker = vendor_symbol.split(".")[0].split(" ")[0]
        else:
            continue
        #
        tiid = symbol.get_tiid_from_bb( bbticker)
        if not tiid:
            print "TradingItemID mapping to exchange ticker {}/{} not found".format(vendor_symbol, vendor)
        else:
            dvdlist = corpaction.get_dividend_info_on_date_range( tiid, date, date)
            if dvdlist == None:
                pass
            else:
                dDict = dict(zip(*tuple(dvdlist)))
                dvd_per_share, dvd_curr =  dDict['divAmount'], dDict['ISOCode']
                dvd_info.append([global_, date, tlsymbol, vendor, vendor_symbol, qty, dvd_per_share, dvd_curr])
        #
    return dvd_info
        

def main(args):
    dvd_data = None
    for acct in args.accts:
        print "Processing TL acct", acct
        holdings = get_holdings_on_daterange( acct, args.start_date, args.end_date, args.global_acct)
        embed()
        dvdinfo = get_dvd_exdate_info_on_holdings( holdings)
        if dvd_data == None:
            dvd_data = dvdinfo[:]
        else:
            dvd_data += dvdinfo[1:] # skip the header
        #
    #
    print '\n'.join(["\t".join(map(str, i)) for i in dvd_data])
    if args.output_file_name != None:
        out_str = '\n'.join([",".join(map(str, i)) for i in dvd_data])
        with open( args.output_file_name, 'w') as outf:
           outf.writelines( out_str)
        
        
        


if __name__ == '__main__':
    parser = argparse.ArgumentParser( description='Demo to get dvd info on holdings', formatter_class=lambda prog: argparse.ArgumentDefaultsHelpFormatter(prog))
    
    parser.add_argument('--accts', '-a', nargs="+", type=str, help='TL account(s) separated by spaces Eg: 2GESTUSDL 2GESTEURL')
    parser.add_argument('--global_acct', '-g' , default=False, action='store_true', help='Global(non US) TL accts if --global_acct arg passed')
    parser.add_argument('--start_date', type=str,  help='Starting date of positions')
    parser.add_argument('--end_date', type=str,  help='Ending date of positions')
    parser.add_argument('--output_file_name', '-f',  type=str, default=None, help='name of output file. The output will be in csv format')
    
    args = parser.parse_args()
    main(args)

