module SA2.Common.Table_index_members

open System

open Utils
open Dates
open DbSchema
open Table_instruments


let missingWeightConstant = -1.0


let rec private partition v =
            
    if List.length v = 0 then

        Map.empty

    else
    
        let ( index , _ , _ , _ ) = v.[0]

        let left , right = v |> List.partition ( fun ( f , _ , _ , _ ) -> f = index )

        let maxDate = left |> List.fold ( fun s ( _ , _ , _ , d ) -> max s d ) DateTime.MinValue

        let ret = 
        
            [ ( index  , 
            
                    left |> List.filter ( fun ( _ , _ , _ , d ) -> d = maxDate )
                         |> List.map ( fun ( _ , t , w , d ) -> ( t , w , d ) )
                )
            ]

            |> Map.ofList
          
        if right.Length <> 0 then

            partition right |> Map.fold ( fun s k v -> Map.add k v s ) ret

        else

            ret




let private mostRecentDatePerIndexId date =

    let multistrategyContext = createMultistrategyContext()

    let indexDates = 

        query {
        
            for row in multistrategyContext.Tbl_index_members do

            select ( row.IndexId , row.Date )

            }
            |> Seq.toArray
            |> Array.filter ( fun ( _ , d ) -> d <= date )

    let indexIds = indexDates |> Array.map ( fun ( i , _ ) -> i ) |> set

    indexDates

        |> Seq.groupBy ( fun ( i , _ ) -> i )
        |> Seq.toArray
        |> Array.map ( fun ( _ , l ) -> l |> Seq.maxBy ( fun ( _ , d ) -> d ) )
        |> Map.ofArray



    
let get_index_members indexes date =
    
    let multistrategyContext = createMultistrategyContext()

    let indexId2Name = getKey_instruments indexes |> Map.toList |> List.unzip |> ( fun ( ns , is ) -> ( is , ns ) ) ||> List.zip |> Map.ofList
    
    let indexId2Date = mostRecentDatePerIndexId date
    
    let allMembers =

        let indexIds = indexId2Name |> keySet

        [|

        for indId in indexIds ->
            
            if Map.containsKey indId indexId2Date then

                let mostRecentDate = Map.find indId indexId2Date  

                query {
            
                    for row in multistrategyContext.Tbl_index_members do

                    where ( row.IndexId = indId && row.Date = mostRecentDate )

                    select row

                }

                |> Seq.toArray

            else

                Array.empty
        |]

        |> Array.concat
    
    let stockId2Name =

        allMembers

            |> Array.fold ( fun s row -> Set.add row.StockId s ) Set.empty

            |> getTicker_instruments
    
    let retInit = ( Set.toList indexes , List.init indexes.Count ( fun i -> List.empty ) ) ||> List.zip |> Map.ofList
    
    allMembers
    
        |> Seq.fold ( 
    
                    fun s row -> 
    
                        let index = Map.find row.IndexId indexId2Name
                    
                        Map.add index ( ( Map.find row.StockId stockId2Name ) :: ( Map.find index s ) ) s
                    
                    ) retInit

    


let getUnion_index_members indexes =
    
    let multistrategyContext = createMultistrategyContext()

    let indexIds = getKey_instruments indexes |> Map.toList |> List.unzip |> snd |> set

    query {

        for row in multistrategyContext.Tbl_index_members do

        select ( row.IndexId , row.StockId )

    }

    |> Seq.filter ( fun ( i , _ ) -> indexIds.Contains i )

    |> Seq.fold ( fun s ( _ , v ) -> Set.add v s ) Set.empty

    |> getTicker_instruments

    |> Map.fold ( fun s _ v -> v :: s ) List.empty




let getForRange_index_members startDate endDate indexes =

    if endDate < startDate then

        raise ( Exception "endDate can't be before startDate" )

    let multistrategyContext = createMultistrategyContext()

    let startDateObj = startDate |> date2Obj
    let endDateObj = endDate |> date2Obj

    let indexIds = getKey_instruments indexes |> Map.toList |> List.unzip |> snd |> set

    query {

        for row in multistrategyContext.Tbl_index_members do

        where ( startDateObj <= row.Date && row.Date <= endDateObj )

        select ( row.IndexId , row.StockId )

    }

    |> Seq.filter ( fun ( i , _ ) -> indexIds.Contains i )

    |> Seq.fold ( fun s ( _ , n ) -> Set.add n s ) Set.empty

    |> getTicker_instruments

    |> Map.fold ( fun s _ t -> t :: s ) List.empty



    
let getOne_index_members index =

    let multistrategyContext = createMultistrategyContext()

    let indexId = [ index ] |> set |> getKey_instruments |> Map.find index

    query {

        for row in multistrategyContext.Tbl_index_members do

        where ( row.IndexId = indexId )

        select ( row.StockId )

    }

    |> set

    |> getTicker_instruments

    |> valueSet




let getAll_index_members () =

    let multistrategyContext = createMultistrategyContext()

    query {

        for row in multistrategyContext.Tbl_index_members do

        select ( row.StockId )

    }

    |> set

    |> getTicker_instruments

    |> valueSet




let getWeights_index_members excludeIndexIfMissingWeights date indexes =
        
    let multistrategyContext = createMultistrategyContext()

    let indexId2Name = getKey_instruments indexes |> Map.toList |> List.unzip |> ( fun ( ns , is ) -> ( is , ns ) ) ||> List.zip |> Map.ofList

    let indexId2Date = mostRecentDatePerIndexId date

    let allMembers =

        let indexIds = indexId2Name |> keySet

        [|

        for indId in indexIds ->

            if Map.containsKey indId indexId2Date then

                let mostRecentDate = Map.find indId indexId2Date  
                let mostRecentDate = Map.find indId indexId2Date 

                query {
            
                    for row in multistrategyContext.Tbl_index_members do

                    where ( row.IndexId = indId && row.Date = mostRecentDate )

                    select ( row.IndexId , row.StockId , row.Weight )

                }

                |> Seq.toArray

            else

                Array.empty

        |]

        |> Array.concat

    let stockId2Name =

        allMembers

            |> Array.map ( fun ( _ , s , _ ) -> s )

            |> set

            |> getTicker_instruments


    allMembers

        |> Array.map ( fun ( i , s , w ) -> ( Map.find i indexId2Name , ( Map.find s stockId2Name , w ) ) )

        |> Seq.groupBy ( fun ( i , _ ) -> i )

        |> Seq.filter ( fun ( _ , l ) -> 

                            let weights = l |> Seq.map ( fun ( _ , ( _ , w ) ) -> w )

                            if excludeIndexIfMissingWeights then
                                weights |> Seq.forall ( fun w -> w <> missingWeightConstant )
                            elif weights |> Seq.forall ( fun w -> w = missingWeightConstant ) |> not then
                                true // so keep the seq and in the next statement remove the names with missing weights -- as opposed to removing the index if excludeIndexIfMissingWeights
                            else false ) 
        
        |> Seq.map ( fun ( i , l ) -> ( i , l |> Seq.filter ( fun ( _ , ( _ , w ) ) -> w <> missingWeightConstant ) |> Seq.map ( fun ( _ , ( s , w ) ) -> ( s , w ) ) |> Seq.toList ) )

        |> Map.ofSeq




let update_index_members tickers2Descr ( indexData : ( int * string * string * float  ) list ) =
    
    let multistrategyContext = createMultistrategyContext()

    let dbTable = multistrategyContext.Tbl_index_members

    // ( date , index , underlying, weight )
    
    let allNames = indexData |> List.fold ( fun s ( _ , i , u , _ ) -> Set.add i s |> Set.add u ) Set.empty

    let allIds =

        try

            getKey_instruments allNames

        with

        | KeysNotFound( s ) -> update_instruments tickers2Descr |> ignore ; getKey_instruments allNames
        
    let indexDataWithIds=

        indexData

            |> List.map ( fun ( d , i , u , w ) -> ( d , Map.find i allIds , Map.find u allIds , w ) )

    let existingRecords =

        query {

            for row in dbTable do

            select ( ( row.IndexId , row.StockId , row.Date |> obj2Date ) , row )

        }

        |> Map.ofSeq

    // update modified records

    indexDataWithIds

        |> List.filter ( fun ( d , i , u , _ ) -> Map.containsKey ( i , u , d ) existingRecords )
        |> List.iter 
            ( 
                fun ( d , i , u , w ) ->

                    let row = Map.find ( i , u , d ) existingRecords
                    if row.Weight <> w 
                        then 
                        row.Weight <- w
            )

    let newRecords = 

        indexDataWithIds

            |> List.filter ( fun ( d , i , u , _ ) -> Map.containsKey ( i , u , d ) existingRecords |> not )
            |> List.map ( fun ( d , i , u , w ) -> new MultistrategySchema.Tbl_index_members( IndexId = i, StockId = u , Weight = w , Date = date2Obj d ) ) 

    multistrategyContext.Tbl_index_members.InsertAllOnSubmit( newRecords )

    try
        
        multistrategyContext.SubmitChanges()

    with

    | e -> raise( e )
