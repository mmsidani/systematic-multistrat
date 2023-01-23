#r@"..\sa2-dlls\common.dll"
#r@"..\sa2-dlls\marketData.dll"



#load ".\settingsProduction.fsx"
#load ".\settingsFxConversion.fsx"
#load ".\settingsIndexes.fsx"
#load ".\settingsDataFieldDictionary.fsx"




open System

open SA2.Common.Dates
open SA2.Common.Dictionaries
open SA2.Common.DbSchema

open SA2.MarketData.BbUpdate



let equitySingleNameData = {

    singleNameMarketDataFields = 
    
        [

        DatabaseFields.longTermBorrowing ;

        DatabaseFields.priceToBook ;

        DatabaseFields.totalEquity ;

        DatabaseFields.returnOnEquity ;

        DatabaseFields.returnOnCapital ;

        DatabaseFields.margin ;

        DatabaseFields.salesPerShare ;

        DatabaseFields.sharesOutstanding ;

        DatabaseFields.operatingIncome ;

        DatabaseFields.ebitda ;

        DatabaseFields.enterpriseValue ;

        DatabaseFields.price ;

        DatabaseFields.bookValue ;

        DatabaseFields.marketCap ;

        DatabaseFields.dailyTotalReturn

        ] ;

    forExDataField =

        [

        DatabaseFields.price

        ] ;

    conversionRateDataField =

        [

        DatabaseFields.yld

        ] ;

    baseCurrencyFxPairs = "USD" ;

    forexIndicator = "CURNCY" ;

}





let globalData = 
    
    { 


    globalDataNamesGics =

        [

        ( "MXWO0AP INDEX" , "251010" ) ;
        ( "MXWO0AU INDEX" , "251020" ) ;
        ( "MXWO0HD INDEX" , "252010" ) ;
        ( "MXWO0LE INDEX" , "252020" ) ;
        ( "MXWO0TA INDEX" , "252030" ) ;
        ( "MXWO0DC INDEX" , "253020" ) ;
        ( "MXWO0HL INDEX" , "253010" ) ;
        ( "MXWO0ME INDEX" , "254010" ) ;
        ( "MXWO0DT INDEX" , "255010" ) ;
        ( "MXWO0NC INDEX" , "255020" ) ;
        ( "MXWO0MR INDEX" , "255030" ) ;
        ( "MXWO0SR INDEX" , "255040" ) ;
        ( "MXWO0FS INDEX" , "301010" ) ;
        ( "MXWO0BV INDEX" , "302010" ) ;
        ( "MXWO0FP INDEX" , "302020" ) ;
        ( "MXWO0TB INDEX" , "302030" ) ;
        ( "MXWO0HH INDEX" , "303010" ) ;
        ( "MXWO0PP INDEX" , "303020" ) ;
        ( "MXWO0EE INDEX" , "101010" ) ;
        ( "MXWO0OG INDEX" , "101020" ) ;
        ( "MXWO0CB INDEX" , "401010" ) ;
        ( "MXWO0TM INDEX" , "401020" ) ;
        ( "MXWO0CK INDEX" , "402030" ) ;
        ( "MXWO0CF INDEX" , "402020" ) ;
        ( "MXWO0DS INDEX" , "402010" ) ;
        ( "MXWO0IR INDEX" , "403010" ) ;
        ( "MXWO0RI INDEX" , "404020" ) ;
        ( "MXWO0RM INDEX" , "404030" ) ;
        ( "MXWO0HE INDEX" , "351010" ) ;
        ( "MXWO0HV INDEX" , "351020" ) ;
        ( "MXWO0HT INDEX" , "351030" ) ;
        ( "MXWO0BT INDEX" , "352010" ) ;
        ( "MXWO0LS INDEX" , "352030" ) ;
        ( "MXWO0PH INDEX" , "352020" ) ;
        ( "MXWO0AD INDEX" , "201010" ) ;
        ( "MXWO0BP INDEX" , "201020" ) ;
        ( "MXWO0CE INDEX" , "201030" ) ;
        ( "MXWO0EL INDEX" , "201040" ) ;
        ( "MXWO0IC INDEX" , "201050" ) ;
        ( "MXWO0MA INDEX" , "201060" ) ;
        ( "MXWO0TD INDEX" , "201070" ) ;
        ( "MXWO0CO INDEX" , "202010" ) ;
        ( "MXWO0PS INDEX" , "202020" ) ;
        ( "MXWO0AF INDEX" , "203010" ) ;
        ( "MXWO0AL INDEX" , "203020" ) ;
        ( "MXWO0MN INDEX" , "203030" ) ;
        ( "MXWO0RR INDEX" , "203040" ) ;
        ( "MXWO0TI INDEX" , "203050" ) ;
        ( "MXWO0ST INDEX" , "453010" ) ;
        ( "MXWO0IF INDEX" , "451020" ) ;
        ( "MXWO0WS INDEX" , "451010" ) ;
        ( "MXWO0SO INDEX" , "451030" ) ;
        ( "MXWO0CQ INDEX" , "452010" ) ;
        ( "MXWO0PC INDEX" , "452020" ) ;
        ( "MXWO0EI INDEX" , "452030" ) ;
        ( "MXWO0OE INDEX" , "452040" ) ;
        ( "MXWO0CH INDEX" , "151010" ) ;
        ( "MXWO0CJ INDEX" , "151020" ) ;
        ( "MXWO0CP INDEX" , "151030" ) ;
        ( "MXWO0MM INDEX" , "151040" ) ;
        ( "MXWO0PF INDEX" , "151050" ) ;
        ( "MXWO0DV INDEX" , "501010" ) ;
        ( "MXWO0WT INDEX" , "501020" ) ;
        ( "MXWO0EU INDEX" , "551010" ) ;
        ( "MXWO0GU INDEX" , "551020" ) ;
        ( "MXWO0IP INDEX" , "551050" ) ;
        ( "MXWO0MU INDEX" , "551030" ) ;
        ( "MXWO0WU INDEX" , "551040" )

        ] ;

    globalDataFields =

        [

        DatabaseFields.marketCap ;
        DatabaseFields.price ;
        DatabaseFields.salesPerShare ;
        DatabaseFields.margin ;

        ]
    }


let equityIndexData = 

    {

    indexUniverse = SettingsIndexes.indexUniverse.indexUniverse ;

    indexMarketDataFields = [ DatabaseFields.price ] ;

    underlyingDataFields = equitySingleNameData

    }




let bbDataInput =

    {

    sourceName = "BB" ;

    numberOfDays = 1200 ; // ran to 20101231

    endDate = 20071022 ; ///////// DateTime.Today |> obj2Date ;

    frequency = DateFrequency.daily ;

    logFilePath = "//SOL/majedTemp/"

    }




// execute

updateEquitiesAndIndexesHistorical SettingsDataFieldDictionary.dataFieldDictionary bbDataInput equityIndexData globalData SettingsFxConversion.fxConversionRates