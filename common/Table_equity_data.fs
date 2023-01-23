module SA2.Common.Table_equity_data


open System
open System.Linq
open Microsoft.FSharp.Data.TypeProviders


open Utils
open Dates
open DbSchema
open Table_instruments
open Table_data_sources
open Table_data_fields




let get_maxChangeId_equity_data_changes ( dbContext : MultistrategySchema.Sa2_multistrategy ) =

    let maxId = 

        query {

            for row in dbContext.Tbl_market_data_changes do

            count

        }

    if maxId = 0 then

        0

    else

        let maxId =

            query {

                for row in dbContext.Tbl_market_data_changes do

                maxBy row.ChangeId

            } 

        maxId
 


    
let private updateCore_equity_data fieldId sourceId divisor dateInstIdValues = 
    
    let multistrategyContext = createMultistrategyContext()

    let today = DateTime.Today
    
    let uniqueInstIds = dateInstIdValues |> Array.map ( fun ( _ , i , _ ) -> i ) |> set

    let changeId = get_maxChangeId_equity_data_changes multistrategyContext |> ref

    for instId in uniqueInstIds do

        let iDateInstIdValues = dateInstIdValues |> Array.filter ( fun ( _ , i , _ ) -> i = instId )
        let iDates = iDateInstIdValues |> Array.map ( fun ( d , _ , _ ) -> d ) 

        let iRecords = 

            query {

                for row in multistrategyContext.Tbl_equity_data do

                where ( fieldId = row.FieldId && instId = row.InstrumentId )

                select ( row )

            }
            |> Seq.toArray
            |> Array.filter ( fun row -> row.Date |> obj2Date |> iDates.Contains )
            |> Array.map ( fun row -> ( ( row.InstrumentId , row.FieldId , row.Date |> obj2Date ) , row ) )
            |> Map.ofArray
        
        let modifiedRecords =     

            iDateInstIdValues 
                |> Array.filter ( fun ( d , i , _ ) -> iRecords.ContainsKey ( i , fieldId , d ) )
                |> Array.map (         
                            fun ( d , i , v ) -> 
                                let row = Map.find ( i , fieldId , d ) iRecords
                                if v - row.Value.Value = 0.0 && row.Divisor = divisor then
                                    None
                                else
                                    Some ( new MultistrategySchema.Tbl_equity_data( InstrumentId = i , FieldId = fieldId , SourceId = sourceId , Value = Nullable<float> v , Date = (d |> date2Obj ), Divisor = divisor ) ,                                     
                                        row )
                            )
                |> Array.filter ( fun p -> p.IsSome )
                |> Array.map ( fun p -> p.Value )
        
        let newRecords =
         
            iDateInstIdValues 
                |> Array.filter ( fun ( d , i , _ ) -> iRecords.ContainsKey ( i , fieldId , d ) |> not )
                |> Array.map ( 
                    fun ( d , i , v ) -> 
                        new MultistrategySchema.Tbl_equity_data( InstrumentId = i , FieldId = fieldId , SourceId = sourceId , Value = Nullable<float> v , Date = (d |> date2Obj ), Divisor = divisor ) )                          
      
        if Array.length modifiedRecords <> 0 then
        
            let toChangeTable = 

                [
                for  ( n , o )  in modifiedRecords ->
                    changeId := !changeId + 1                        
                    let recordChange =                                
                        new MultistrategySchema.Tbl_market_data_changes(
    
                                                    InstrumentId = n.InstrumentId, 
                                                    FieldId = n.FieldId, 
                                                    Date = n.Date, 
                                                    OldValue = o.Value, 
                                                    NewValue = n.Value, 
                                                    DateOfChange = today, 
                                                    OldSourceId = o.SourceId, 
                                                    NewSourceId = n.SourceId,
                                                    OldDivisor = o.Divisor,
                                                    NewDivisor = n.Divisor ,
                                                    ChangeId = !changeId )
                    o.Value <- n.Value
                    o.SourceId <- n.SourceId
                    o.Divisor <- n.Divisor

                    recordChange
                ]

            multistrategyContext.Tbl_market_data_changes.InsertAllOnSubmit( toChangeTable )
    
        multistrategyContext.Tbl_equity_data.InsertAllOnSubmit( newRecords )
    
    try
        
        multistrategyContext.SubmitChanges()

    with

    | e -> raise( e )



    
let updateExisting_equity_data sourceName sa2FieldName divisor dateTickerValues =

    let sourceId = 
        
        try
            
            getKey_data_sources ( Set.ofList [ sourceName ] ) 

        with

        | KeysNotFound( name ) -> raise ( Exception ( "source " + sourceName + " doesn't exist. existing sources only in this function" ) )

        |> Map.find sourceName

    let fieldId = 

        try
            
            getKey_data_fields true ( Set.ofList [ sa2FieldName ] ) 

        with

        | KeysNotFound( ids ) -> raise ( Exception ( "field " + sa2FieldName + " doesn't exist. existing fields only in this function" ) )

        |> Map.find sa2FieldName

    let instIds = 

        try 

            dateTickerValues |> Array.map ( fun ( _ , t , _ ) -> t ) |> set |> getKey_instruments 

        with

        | KeysNotFound( ids ) -> raise ( Exception ( "tickers " + ids.ToString() + " don't exist. existing instruments only in this function" ) )

    dateTickerValues |> Array.map ( fun ( d , t , v ) -> ( d , Map.find t instIds , v ) ) |> updateCore_equity_data fieldId sourceId divisor




let update_equity_data sourceName sa2FieldName field divisor dateDescrTickerValues =

    let sourceId = 
        
        try
            
            getKey_data_sources ( Set.ofList [ sourceName ] )

        with

        | KeysNotFound( name ) -> update_data_sources name ||> List.zip |> Map.ofList

        |> Map.find sourceName

    let fieldId = 

        try
            
            getKey_data_fields true ( Set.ofList [ sa2FieldName ] ) 

        with

        | KeysNotFound( ids ) -> [ (sa2FieldName , field ) ] |> Map.ofList |> update_data_fields ||> List.zip |> Map.ofList

        |> Map.find sa2FieldName

    let instIds = 

        try 

            dateDescrTickerValues |> Array.map ( fun ( _ , _ , t , _ ) -> t ) |> set |> getKey_instruments

        with

        | KeysNotFound( ids ) -> dateDescrTickerValues |> Array.map ( fun ( _ , dsc , t , _ ) -> ( t , dsc ) ) |> Map.ofArray |> update_instruments ||> List.zip |> Map.ofList
   
    dateDescrTickerValues |> Array.map ( fun ( d , _ , t , v ) -> ( d , Map.find t instIds , v ) ) |> updateCore_equity_data fieldId sourceId divisor
    
    
    
    
let private getCore_equity_data sa2FieldName tickers numRecords date =
    
    let multistrategyContext = createMultistrategyContext()

    let instIdsMap = 

        try 

            getKey_instruments ( Set.ofList tickers )

        with

        | KeysNotFound( names ) -> raise( Exception( "some tickers are not known: " + paste( Set.toList names ) ) )
 

    let fieldIdsMap =

        try 

            getKey_data_fields true ( set [ sa2FieldName ] )

        with

        | KeysNotFound( names ) -> raise( Exception( "some fields are not known: " + paste( Set.toList names ) ) )
    
    let uniqueInstIds = instIdsMap |> valueSet
    let fieldId = Map.find sa2FieldName fieldIdsMap
    let id2Ticker = instIdsMap |> Map.toList |> List.map ( fun ( t , i ) -> ( i , t ) ) |> Map.ofList
    
    let ret = Map.empty |> ref

    for instId in uniqueInstIds do

        let iData =

            if numRecords = -1 then

                query {

                    for row in multistrategyContext.Tbl_equity_data do

                    where ( fieldId = row.FieldId && instId = row.InstrumentId )

                    select ( row.Date, row.Value , row.Divisor )

                }

            else

                query {

                    for row in multistrategyContext.Tbl_equity_data do

                    where ( fieldId = row.FieldId && instId = row.InstrumentId && row.Date <= date )

                    sortByDescending row.Date

                    select ( row.Date, row.Value , row.Divisor )

                    take numRecords

                }

            |> ( fun sq ->                
                    Seq.toList sq
                        |> List.filter ( fun ( _ , v , _ ) -> v.HasValue )
                        |> List.map ( fun ( dt , v , dv ) -> ( dt |> obj2Date , v.Value / dv ) ) )

        ret := if iData.IsEmpty |> not then Map.add ( Map.find instId id2Ticker ) iData !ret else !ret

    !ret




let get_equity_data sa2FieldName tickers =

    let mktData = getCore_equity_data sa2FieldName tickers -1 DateTime.MinValue

    if Map.isEmpty mktData then

        Map.add sa2FieldName Map.empty Map.empty

    else
        
        Map.add sa2FieldName mktData Map.empty




let getForDate_equity_data numRecords date sa2FieldName tickers =

    let mktData = getCore_equity_data sa2FieldName tickers numRecords date

    if Map.isEmpty mktData then

        Map.add sa2FieldName Map.empty Map.empty

    else
        
        Map.add sa2FieldName mktData Map.empty





