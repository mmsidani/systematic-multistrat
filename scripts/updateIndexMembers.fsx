#r@"..\sa2-dlls\common.dll"
#r@"..\sa2-dlls\equities.dll"



#load ".\settingsProduction.fsx"
#load ".\settingsIcb2Gics.fsx"
#load ".\settingsIndexes.fsx"


open System

open SA2.Common.DbSchema
open SA2.Common.Dates

open SA2.Common.FxTypes
open SA2.Common.Dictionaries

open SA2.Equity.EquityUniverse
open SA2.Equity.IndexMembers



let private forex = {

    forexIndicator = "CURNCY" ;
    forexRateField = "PX_LAST";

}




let dataFieldDictionary = {

    bbStaticField2Sa2Name = 
    
        [

        ( "CRNCY" , DatabaseFields.currency ) ;

        ( "GICS_INDUSTRY" , DatabaseFields.gicsIndustry ) ;

        ( "SHORT_NAME" , DatabaseFields.shortName ) ;

        ( "EQY_FUND_TICKER" , DatabaseFields.fundamentalTicker ) ;

        ]

        |> Map.ofList ;

}




let equityUniverseInput = {

    baseCurrency = "USD";

    indexes = SettingsIndexes.indexUniverse.indexUniverse        

    equityClassIndicator = "EQUITY" ;

    marketCapField = "CUR_MKT_CAP" ;

    gicsField = "GICS_INDUSTRY" ;

    currencyField = "CRNCY" ;

    shortNameField = "SHORT_NAME" ;
    
    fundamentalTickerField =  "EQY_FUND_TICKER" ;

    icbField = "ICB_SUBSECTOR_NUM" ;

    numberToKeep = 4000 ;

    sourceName = "BB"

}




let indexMembersInput = {

    equityUniverseInput = equityUniverseInput ;

    indexes = equityUniverseInput.indexes ;

    indexMembersField = "INDX_MWEIGHT_HIST" ;

    logFilePath = "//Sol/majedTemp/"

}




let indexMembersHistoricalInput = {

    indexMembersInput = indexMembersInput ;

    startDate = 20050101 ;

    endDate = 20101231 ;

    frequency = DateFrequency.monthly ;

}



try
     
    updateIndexMembersHistorical indexMembersHistoricalInput forex dataFieldDictionary SettingsIcb2Gics.icb2GicsConverter
    
with

| e -> e.ToString() |> printfn "%s" 


