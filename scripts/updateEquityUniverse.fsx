#r@"..\sa2-dlls\common.dll"


#load ".\settingsProduction.fsx"
#load ".\settingsIndexes.fsx"


open System

open SA2.Common.DbSchema
open SA2.Common.Dates
open SA2.Common.FxTypes
open SA2.Common.Dictionaries

open SA2.Equity.EquityUniverse



let private forex = {

    forexIndicator = "CURNCY" ;
    forexRateField = "PX_LAST";

}




let dataFieldDictionary = {

    bbStaticField2Sa2Name = 
    
        [

        ( "CRNCY" , DatabaseFields.currency ) ;

        ( "GICS_INDUSTRY" , DatabaseFields.gicsIndustry ) ;

        ( "SHORT_NAME" , DatabaseFields.shortName )

        ]

        |> Map.ofList ;

}




let equityUniverseInput = {

    baseCurrency = "USD";

    indexes = SettingsIndexes.indexUniverse.indexUniverse

    equityClassIndicator = "EQUITY" ;

    marketCapField = "CUR_MKT_CAP" ;

    gicsField = "" ;

    currencyField = "" ;

    shortNameField = "" ;

    fundamentalTickerField = "" ;

    icbField = "" ;
              
    numberToKeep = 5000 ;

    sourceName = "BB"

}




let equityUniverseHistoricalInput = {

    equityUniverseInput = equityUniverseInput ;

    startDate = 20030101 ;

    endDate = 20131231 ;

    frequency = DateFrequency.monthly ;

}



try
     
    updateEquityUniverseHistorical equityUniverseHistoricalInput forex dataFieldDictionary
    
with

| e -> e.ToString() |> printfn "%s" 


