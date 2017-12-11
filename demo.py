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


def main(args):
    #spx = [i[0] for i in read_csv("spx.csv")]
    for tkr in args.symbols:
        tiid = symbol.get_tiid_from_bb( tkr, args.global_ticker)
        if tiid == None:
            print tkr, "MISSING MAPPING - Cannot find TradingItemID ----------------------------"
        else:
            dvd_exday = corpaction.check_if_current_day_is_dvd_exdate( tiid)
            split_exday = corpaction.check_if_current_day_is_split_exdate( tiid)
            curr_evts = corpaction.get_all_upcoming_events( tiid)
            #########################################################
            # Any specific can be extracted by event id
            #
            # Events and  Event Type ID
            #
            # 48 Earnings Calls
            # 49 Guidance/Update Calls
            # 50 Shareholder/Analyst Calls
            # 51 Company Conference Presentations
            # 52 M&A Calls
            # 55 Earnings Release Date
            # 62 Annual General Meeting
            # 78 Board Meeting
            # 97 Special/Extraordinary Shareholders Meeting
            # 139 Sales/Trading Statement Calls
            # 140 Sales/Trading Statement Release Date
            # 144 Estimated Earnings Release Date (S&P Global Derived)
            # 149 Conferences
            # 192 Analyst/Investor Day
            # 194 Special Calls
            # 219 Operating Results Release Date
            # 220 Interim Management Statement Release Date
            # 221 Operating Results Calls
            # 222 Interim Management Statement Calls
            # 223 Fixed Income Calls
            #
            # ern_evts = corpaction.get_upcoming_events_by_type(tiid, 48)
            #
            #########################################################
            
            if dvd_exday or split_exday or curr_evts:
                print tkr, tiid, dvd_exday, split_exday, curr_evts, ern_evts
    


if __name__ == '__main__':
    parser = argparse.ArgumentParser( description='Demo to get corp actions', formatter_class=lambda prog: argparse.ArgumentDefaultsHelpFormatter(prog))
    
    parser.add_argument('--symbols', '-s', nargs="+", type=str, help='Bloomberg symbols(s) separated by spaces Eg: AMZN XOM')

    parser.add_argument('--global_ticker', '-g' , default=False, action='store_true', help='Global(non US) ticker')
    parser.add_argument('--output_file_name',  '-o', type=str, default=None, help='name of output file. The output will be in csv format')
    
    args = parser.parse_args()
    main(args)


