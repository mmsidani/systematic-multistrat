#r@"..\sa2-dlls\common.dll"
#r@"..\sa2-dlls\equities.dll"
#r@"..\sa2-dlls\marketData.dll"
#r@"..\sa2-dlls\MathNet.Numerics.dll"


open System
open SA2.Common.Io
open SA2.Common.Table_market_data
open SA2.Common.Table_sa2_macro_data
open SA2.Common.DbSchema
open SA2.Common.Returns
open SA2.Common.Dates
open SA2.Common.KMeans
open SA2.Common.Math
open SA2.Common.Utils

open SA2.Equity.MacroModels


open SA2.MarketData.BloombergHistorical


let index2GdpRate =

    [

        (  "AS51 INDEX"  , "au.gdprate" ) ;
        (  "SPTSX60 INDEX"  , "ca.gdprate" ) ;
        (  "SMI INDEX"  , "sz.gdprate" ) ;
        (  "IBEX INDEX"  , "sp.gdprate" ) ;
        (  "DAX INDEX" , "ge.gdprate" ) ;
        (  "CAC INDEX" , "fr.gdprate" ) ;
        (  "NIFTY INDEX"  , "in.gdprate" ) ;
        (  "FTSEMIB INDEX" , "it.gdprate" ) ;
        (  "TPX INDEX", "jp.gdprate" ) ;
        (  "KOSPI2 INDEX"  , "ko.gdprate" ) ;
        (  "MEXBOL INDEX" , "mx.gdprate" ) ;
        (  "OMX INDEX" , "sw.gdprate" ) ;
        (  "UKX INDEX"  , "uk.gdprate" ) ;
        (  "INDU INDEX"   , "us.gdprate" ) ;
        (  "TOP40 INDEX"  , "sa.gdprate" ) ;
        (  "SX5E INDEX"   , "eu.gdprate" ) ;
        (  "NKY INDEX"   , "jp.gdprate" ) ;            
        (  "SPX INDEX"   , "us.gdprate" ) ;            
        (  "NDX INDEX"   , "us.gdprate" ) ;            
        (  "RTY INDEX"   , "us.gdprate" ) ;           
        (  "AEX INDEX"   , "nl.gdprate" ) ;          
        (  "OBXP INDEX"   , "no.gdprate" ) ;          
        (  "WIG20 INDEX"   , "pl.gdprate" ) ;         
        (  "RTSI$ INDEX"   , "ru.gdprate" ) ;          
        (  "IBOV INDEX"   , "bz.gdprate" ) ;
        (  "XU030 INDEX"   , "tk.gdprate" ) ;
        (  "TAMSCI INDEX" , "tw.gdprate" ) ;
        ( "HSCEI INDEX" , "ch.gdprate" ) ;
        ( "FBMKLCI INDEX" , "my.gdprate" ) ;
        ( "SET50 INDEX" , "th.gdprate" ) ;
        ( "SIMSCI INDEX"  , "sg.gdprate")
        ( "HSI INDEX"  , "hk.gdprate")

    ]

    |> Map.ofList

let investableEquities =

    [

        "DAX INDEX" ;
        "CAC INDEX" ;
        "UKX INDEX" ;
        "OMX INDEX" ;
        "SXXP INDEX" ;
        "TPX INDEX" ;
        "AS51 INDEX" ;
        "SIMSCI INDEX" ;
        "SPTSX60 INDEX" ;
        "HSI INDEX" ;
        "AEX INDEX" ;
        "IBEX INDEX" ;
        "FTSEMIB INDEX" ;
        "OBXP INDEX" ;
        "SMI INDEX" ;
        "TAMSCI INDEX";
        "SX5E INDEX" ;
        "INDU INDEX" ;
        "NKY INDEX" ;
        "SPX INDEX" ; 
        "NDX INDEX" ;
        "KOSPI2 INDEX" ;
        "RTY INDEX" ;
        "NIFTY INDEX" ;
        "SET50 INDEX" ;
        "FBMKLCI INDEX" ;
        "WIG20 INDEX" ;
        "TOP40 INDEX" ;
        "HSCEI INDEX" ;
        "RTSI$ INDEX" ; 
//        "XIN9I INDEX"; 
        "MEXBOL INDEX"; 
        "IBOV INDEX" ; 
        "XU030 INDEX"; 
//        "SHSN300 INDEX";

    ]


let arrayPaste a =

    a |> Array.fold ( fun s v -> s + "," + v.ToString() ) ""


let calcRets insts =

    get_market_data DatabaseFields.price insts |> Map.find DatabaseFields.price

        |> Map.map ( fun _ l -> calculateReturns l |> List.toArray )
        |> Map.toArray
        |> Array.map ( fun ( i , a ) -> a |> Array.map ( fun ( d , v ) -> ( d , i , v ) ) )
        |> Array.concat
        |> Seq.groupBy ( fun ( d , _ , _ ) -> d )
        |> Seq.toArray
        |> Array.map ( fun ( d , s ) -> ( d , s |> Seq.toArray ))



let corClusters numPeriods minPerCluster numClusters startDate endDate dateFreq descriptors =

    let dates = datesAtFrequency dateFreq startDate endDate |> Array.rev

    let minDate , maxDate = descriptors |> Array.map ( fun ( d , _ ) -> d ) 
                                    |> ( fun l -> ( Array.reduce ( fun d0 d1 -> min d0 d1 ) l , Array.reduce ( fun d0 d1 -> max d0 d1 ) l ) )
    let descDates = datesAtFrequency dateFreq minDate maxDate |> set |> Set.filter ( fun d -> d <= endDate )
    let filteredDescs = descriptors |> Array.filter ( fun ( d , _ ) -> descDates.Contains d ) |> Array.sortBy ( fun ( d , _ ) -> d )  |> Array.rev // decreasing dates
    
    let corrDist x y =

        correlation x y

    [|        
  
    for i in 0 .. dates.Length-1 ->
        
        let date , dRets = filteredDescs.[ i ]
        printfn "now doing %d" date
        let dData =

            filteredDescs.[ i .. ( i + numPeriods - 1 ) ]

                |> Array.map ( fun ( _ , a ) -> a )
                |> Array.concat
                |> Seq.groupBy ( fun ( _ , i , _ ) -> i )
                |> Seq.toArray
                |> Array.map ( fun ( i , s ) -> 
                                ( i ,
                                        s 
                                            |> Seq.toArray 
                                            |> Array.sortBy ( fun ( d , _ , _ ) -> d ) 
                                            |> Array.map ( fun ( _ , _ , v ) -> v ) )
                            )

                |> Map.ofArray
                |> Map.filter ( fun jjj a -> a.Length = numPeriods )
 
        let clusters = computeClusters false minPerCluster numClusters corrDist dData

        let centroids =
                clusters
                    |> Array.map ( fun s -> 
                                    dData 
                                        |> Map.filter ( fun n _ -> Set.contains n s ) 
                                        |> Map.toArray
                                        |> Array.map ( fun ( _ , a ) -> a ) 
                                        |> computeCentroid )

        let corrsWithCentoids =
            clusters
                |> Array.mapi ( fun i s -> 
                                    s 
                                        |> Set.map ( fun n -> 
                                                        ( n , 
                                                            let nDData = Map.find n dData

                                                            Array.append 
                                                                [| nDData |> correlation centroids.[ i ] |]                                                
                                                                [| for j in 0 .. centroids.Length - 1 ->
                                                                        correlation nDData centroids.[j] |] ) ) 
                                        |> Set.toArray )

        let interCentroidsCorrs =

            centroids
                |> Array.mapi ( fun i c -> 
                                    [|
                                        for j in i+1 .. centroids.Length-1 ->
                                            correlation c centroids.[ j ]
                                    |]
                )

        ( date , ( corrsWithCentoids , interCentroidsCorrs ) )

    |]

    |> Map.ofArray




let gdpQoq inst2GdpName =

    let gdpRateNames = inst2GdpName |> valueSet
    let gdpData = gdpRateNames |> Set.toList |> get_sa2_macro_data |> buildGdpLevelsFromYoy

    gdpData 
        |> Map.map ( fun _ l -> 
                        l 
                            |> List.sort 
                            |> Seq.pairwise 
                            |> Seq.map ( fun ( ( d0 , v0 ) , ( d1, v1) ) -> ( d1 |> obj2Date , v1 / v0 - 1.0 ) )
                            |> Seq.toArray )
        |> Map.toArray
        |> Array.map ( fun ( i , a ) -> a |> Array.map ( fun ( d , v ) -> ( d , i , v ) ) )
        |> Array.concat
        |> Seq.groupBy ( fun ( d , _ , _ ) -> d )
        |> Seq.toArray
        |> Array.map ( fun ( d , s ) -> ( d , s |> Seq.toArray ))





// execute

let results = index2GdpRate |> gdpQoq |> corClusters 20 4 10 20131231 20131231 DateFrequency.daily

results

    |> Map.iter ( fun d ( nc , cc ) -> 
                    printfn "%d\n" d ;
                    printfn "corr instrument with centroid"
                    nc |> Array.iteri ( fun i s -> printfn "%d-th:" i ; s |> Array.map ( fun ( n , a ) -> n + "," + (a |> arrayPaste) ) |> Array.iter ( fun s -> printfn "%s"  s ) ) 
                    printfn "corr centroid with centroid"
                    cc |> Array.iteri ( fun i s -> s |> paste |> printfn "%d-th: \n%s" i ) )
