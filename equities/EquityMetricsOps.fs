module SA2.Equity.EquityMetricsOps

open System

open SA2.Common.Dates
open SA2.Common.Data
open SA2.Common.Utils
open SA2.Common.DbSchema
open SA2.Common.ForEx


open EquityMetrics


// MissingDataAlert: missing data does not kill the computations. see hasAllData



let private hasAllData n dataMaps =

    dataMaps |> List.fold ( fun s m -> s && Map.containsKey n m ) true




let industryCharacteristicOp ( op : float list -> float ) characteristic partitionedSecs marketData =

    // staticData assumed to be output from a yet to be created static data table which outputs Map< string , (string * string) list as opposed to market_data's Map< string, (int * string *float) list >
    // aggregationFields = [ "gics" ; "index" ; "currency" ] for example

    let ( characteristicMap : Map< string , float > )= Map.find characteristic marketData |> Map.ofList

    let partSums = partitionedSecs |> List.map ( fun l -> l |> List.map ( fun s -> Map.find s characteristicMap  ) )  |> List.map ( fun l -> op l )

    ( partitionedSecs ,  partSums )




let characteristicGrowth characteristic partitionedSecs marketData date1 date2 =

    // if there's need for currency conversion, then it should be done prior to calling characteristicGrowth

    let _ , revenue1 = industryCharacteristicOp 
    
                            List.sum                      
                            characteristic
                            partitionedSecs
                            ( marketData |> Map.map ( fun k v  -> ( v |> List.filter ( fun ( d , _ , _ ) -> d = date1 ) ) |> List.map ( fun ( _ , t , v ) -> ( t , v )  ) ) )
    
    
    let _ , revenue2 = industryCharacteristicOp 
    
                            List.sum                      
                            characteristic
                            partitionedSecs
                            ( marketData |> Map.map ( fun k v  -> ( v |> List.filter ( fun ( d , _ , _ ) -> d = date2 ) ) |> List.map ( fun ( _ , t , v ) -> ( t , v )  ) ) )


    List.map2 ( fun r1 r2 -> r2 / r1 - 1.0 ) revenue1 revenue2




let industryAverageSalesGrowth input =

    ()




let industryWorstGrowthRate input =

    ()




let industryBestGrowthRate input =

    ()


let periodChange dataForDate dataForPriorDate names =

    let namesSet = names |> Seq.filter ( fun n -> hasAllData n [ dataForDate ; dataForPriorDate ] ) |>  set 
            
    namesSet

        |> Set.fold ( 
        
                     fun s g ->
        
                        let prior : float = Map.find g dataForPriorDate
                        
                        if prior = 0.0 then
                        
                             s

                        else
                            
                            Map.add g ( ( Map.find g dataForDate - prior ) / prior ) s

                    ) 
                    
                    Map.empty




let calculateTotalSales useLocalCurrency forexData data date names = 

    let salesPerShare : Map< string , float > = getDbDataForDate data date DatabaseFields.salesPerShare
    
    let outstandingShares = getDbDataForDate data date DatabaseFields.sharesOutstanding
    
    names 
    
        |> Seq.filter ( fun n -> hasAllData n [ salesPerShare ; outstandingShares ; forexData ] )

        |> Seq.map ( fun n -> ( n , Map.find n salesPerShare * Map.find n outstandingShares ) )
        
        |> ( fun s -> 
        
                if useLocalCurrency then

                    s

                else 
                
                    s |> Seq.map ( fun ( n , sales ) -> ( n , sales * Map.find n forexData ) )
            )  
    
        |> Map.ofSeq




let calculateMarketShares data date name2Gics forexData names =    

    // use the members of a gics as a proxy for total industry size

    let totalSales = calculateTotalSales false forexData data date names

    let filteredNames = totalSales |> keySet |> Set.toList

    let gics = filteredNames 
                |> List.map ( fun k -> Map.find k name2Gics ) |> set |> Set.toList

    let salesPerGicsInit = ( gics , List.init gics.Length ( fun i -> 0.0 ) ) ||> List.zip |> Map.ofList
    
    let salesPerGics = 
    
        totalSales 
        
        |> Map.fold ( fun s n v -> 

                            let g = Map.find n name2Gics

                            Map.add g ( Map.find g s + v ) s ) salesPerGicsInit

    ( filteredNames , 
    
        filteredNames |> List.map ( fun n -> Map.find n totalSales / ( Map.find ( Map.find n name2Gics) salesPerGics ) ) ) 

            ||> List.zip

            |> Map.ofList




let calculateProfitability useConversion baseCurrency data conversionData date names name2Currency =

    let roe = getDbDataForDate data date DatabaseFields.returnOnCapital

    let conversionFactors = convertToPerspective baseCurrency conversionData date

    names

        |> Seq.filter ( fun n -> hasAllData n [ roe  ] )

        |> Seq.map ( fun n -> ( n , Map.find n roe ) ) 

        |> ( fun ret ->
                if useConversion then
                    ret |> Seq.map ( fun ( n , v ) -> 
                                        if Map.containsKey n name2Currency |> not then
                                            printfn "no currency for %s" n
                                        elif Map.containsKey (Map.find n name2Currency) conversionFactors |> not then
                                            printfn "no conversion for ccy %s" ( Map.find n name2Currency )
                                        ( n , v * ( Map.find ( Map.find n name2Currency ) conversionFactors ) ) )
                else ret ) 

        |> Map.ofSeq




let calculateEvEbitdaMultiple useConversion baseCurrency data conversionData date names name2Currency =

    let getDbData = getDbDataForDate data date

    let enterpriseValues : Map< string , float > =  getDbData DatabaseFields.enterpriseValue

    let ebitda = getDbData DatabaseFields.ebitda

    let conversionFactors = convertToPerspective baseCurrency conversionData date

    let namesWithMissingData = names |> Seq.filter ( fun n -> hasAllData n [ enterpriseValues ; ebitda ] |> not )

    let priceToBook , longTermBorrow , bookValue , operIncome = 
    
        if namesWithMissingData |> Seq.length <> 0 then 
        
            getDbData DatabaseFields.priceToBook , getDbData DatabaseFields.longTermBorrowing , getDbData DatabaseFields.bookValue , getDbData DatabaseFields.operatingIncome
            
        else 
        
            Map.empty , Map.empty , Map.empty , Map.empty 

    let ebitdaEv = 

        names

            |> Seq.filter ( fun n -> hasAllData n [ enterpriseValues ; ebitda ] )

            |> Seq.map ( fun n -> ( n , Map.find n ebitda / Map.find n enterpriseValues ) )
            
            |> ( fun ret ->
                    if useConversion then
                        ret |> Seq.map ( fun ( n , v ) -> ( n , v * ( Map.find ( Map.find n name2Currency ) conversionFactors ) ) )
                    else ret ) 

            |> Map.ofSeq
            
    // but ev and ebitda could be missing, so...

    let operIncPrice =

        namesWithMissingData

            |> Seq.filter ( fun n -> hasAllData n [ operIncome ; priceToBook ; longTermBorrow ; bookValue ] )

            |> Seq.map ( fun n -> ( n , Map.find n operIncome / ( Map.find n priceToBook * Map.find n bookValue + Map.find n longTermBorrow )  ) )

            |> ( fun ret ->
                    if useConversion then
                        ret |> Seq.map ( fun ( n , v ) -> ( n , v * ( Map.find ( Map.find n name2Currency ) conversionFactors ) ) )
                    else ret )

            |> Map.ofSeq

    Map.fold ( fun s k v -> Map.add k v s ) ebitdaEv operIncPrice




let calculateEvEbitMultiple useConversion baseCurrency data conversionData date names name2Currency =

    let getDbData = getDbDataForDate data date

    let enterpriseValues : Map< string , float > =  getDbData DatabaseFields.enterpriseValue

    let ebit = getDbData DatabaseFields.ebit

    let conversionFactors = convertToPerspective baseCurrency conversionData date

    let namesWithMissingData = names |> Seq.filter ( fun n -> hasAllData n [ enterpriseValues ; ebit ] |> not )

    names

        |> Seq.filter ( fun n -> hasAllData n [ enterpriseValues ; ebit ] )

        |> Seq.map ( fun n -> ( n , Map.find n ebit / Map.find n enterpriseValues ) )
            
        |> ( fun ret ->
                if useConversion then
                    ret |> Seq.map ( fun ( n , v ) -> ( n , v * ( Map.find ( Map.find n name2Currency ) conversionFactors ) ) )
                else ret ) 

        |> Map.ofSeq