#r@"..\sa2-dlls\common.dll"



open SA2.Common.DbSchema
open SA2.Common.Dictionaries




let dataFieldDictionary = {

    bbField2Sa2NameDivisor = 
    
        [

        ( "BS_LT_BORROW" , DatabaseFields.longTermBorrowing , 1.0 ) ;

        ( "PX_TO_BOOK_RATIO" , DatabaseFields.priceToBook , 1.0 ) ;

        ( "TOTAL_EQUITY" , DatabaseFields.totalEquity , 1.0 ) ;

        ( "RETURN_COM_EQY" , DatabaseFields.returnOnEquity , 100.0 ) ;

        ( "RETURN_ON_CAP" , DatabaseFields.returnOnCapital , 100.0 ) ;

        ( "TRAIL_12M_OPER_MARGIN" , DatabaseFields.margin , 100.0 ) ;

        ( "TRAIL_12M_SALES_PER_SH" , DatabaseFields.salesPerShare , 1.0 ) ;

        ( "TRAIL_12M_EPS" , DatabaseFields.earnings , 1.0 ) ;

        ( "EQY_DVD_YLD_12M" , DatabaseFields.dividendYield , 100.0 ) ;

        ( "EQY_SH_OUT" , DatabaseFields.sharesOutstanding , 1.0 ) ;

        ( "TRAIL_12M_OPER_INC" , DatabaseFields.operatingIncome , 1.0 ) ;

        ( "EBITDA" , DatabaseFields.ebitda , 1.0 ) ;

        ( "EBIT" , DatabaseFields.ebit , 1.0 ) ;

        ( "CURR_ENTP_VAL" , DatabaseFields.enterpriseValue , 1.0 ) ;

        ( "PX_LAST" , DatabaseFields.price , 1.0 ) ;

        ( "PX_LAST" , DatabaseFields.yld , 100.0 ) ;

        ( "BOOK_VAL_PER_SH" , DatabaseFields.bookValue , 1.0 ) ;

        ( "CUR_MKT_CAP" ,  DatabaseFields.marketCap , 1.0 ) ;

        ( "DAY_TO_DAY_TOT_RETURN_GROSS_DVDS" , DatabaseFields.dailyTotalReturn , 100.0 ) 

        ]


    nameFieldDivisorException =

        [

        // ( "XYZUSD CURNCY" , f ) means that the market convention is to quote price of f XYZ for 1 USD

        ( "KRWUSD CURNCY" , DatabaseFields.price , 100.0 ) ;

        ( "IDRUSD CURNCY" , DatabaseFields.price , 1000.0 ) ;

        ( "CLPUSD CURNCY" , DatabaseFields.price , 100.0 ) ;

        ( "COPUSD CURNCY" , DatabaseFields.price , 100.0 )

        ]
}
