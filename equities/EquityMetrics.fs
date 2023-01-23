module SA2.Equity.EquityMetrics

open System
open SA2.Common.Dates
open SA2.Common.Data
open SA2.Common.DbSchema




let globalInflation months data date =

    // assumes field is called "numeraireLevel" and variable is "numeraire"

    if Map.containsKey "numeraireLevel" data |> not then

        raise ( Exception( "no numeraire level data was passed." ) )

    let numeraireData : Map< string, ( int * float ) list > = Map.find "numeraireLevel" data

    let priorDate = ( date |> date2Obj ).AddMonths( - months ) |> obj2Date

    let valueForDate dt =

        numeraireData 

            |> lastDataMap dt
            |> Map.find "numeraire"

    let dateLevel = valueForDate date        

    let priorDateLevel =  valueForDate priorDate

    ( dateLevel - priorDateLevel ) / priorDateLevel




let calculateGlobalIndustrySize data date =
    
    if List.forall ( fun k -> Map.containsKey k data ) [ DatabaseFields.marketCap ; DatabaseFields.price ] then
        
        let marketCaps = Map.find DatabaseFields.marketCap data |> lastDataMap date
        
        let prices = Map.find DatabaseFields.price data |> lastDataMap date
        
        let globalSalesPerShare = Map.find DatabaseFields.salesPerShare data |> lastDataMap date
        
        marketCaps

            |> Map.map ( fun k ( v : float ) -> v / Map.find k prices * Map.find k globalSalesPerShare )

    else
        
        Map.empty




let getDbDataForDate marketData date databaseFieldName  =

    if Map.containsKey databaseFieldName marketData then
        
        Map.find databaseFieldName marketData |> lastDataMap date

    else
        
        Map.empty