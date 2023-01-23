module SA2.Common.Table_static_data

open System

open Utils
open Dates
open DbSchema
open Table_instruments
open Table_data_sources
open Table_data_fields




let rec private partition v =
            
    if List.length v = 0 then

        Map.empty

    else

        let ( field , _ , _  ) = v.[0]

        let left , right = v |> List.partition ( fun ( f , _ , _ ) -> f = field )

        let ret = [ ( field  , left |> List.map ( fun ( _ , t , v ) -> ( t , v ) ) ) ] |> Map.ofList

        if right.Length <> 0 then

            partition right |> Map.fold ( fun s k v -> Map.add k v s ) ret

        else

            ret




let private updateCore_static_data ( records : MultistrategySchema.Tbl_static_data list ) =
    
    let multistrategyContext = createMultistrategyContext()

    let existingRecords =

        query {

            for row in multistrategyContext.Tbl_static_data do

            select ( ( row.InstrumentId , row.FieldId ) , row )

        }

        |> Map.ofSeq

    for record in records do

        let recordKey = ( record.InstrumentId , record.FieldId )

        match Map.containsKey recordKey existingRecords
                                  
            with

        | true -> let er = Map.find recordKey existingRecords

                  er.SourceId <- record.SourceId

                  er.Value <- record.Value

        | false -> multistrategyContext.Tbl_static_data.InsertOnSubmit( record )

    try
        
        multistrategyContext.SubmitChanges()

    with

    | e -> raise( e )



let updateExisting_static_data sourceName sa2FieldName tickers values =

    let sourceId = 
        
        try
            
            getKey_data_sources ( set [ sourceName ] ) 

        with

        | KeysNotFound( name ) -> raise ( Exception ( "source " + name.ToString() + " doesn't exist. existing sources only in this function" ) )

        |> Map.find sourceName

    let fieldId = 

        try
            
            getKey_data_fields true ( set [ sa2FieldName ] ) 

        with

        | KeysNotFound( name ) -> raise ( Exception ( "field " + name.ToString() + " doesn't exist. existing fields only in this function" ) )

        |> Map.find sa2FieldName

    let instIds = 

        try 

            getKey_instruments ( set tickers )

        with

        | KeysNotFound( ids ) -> raise ( Exception ( "these tickers " + ids.ToString() + " don't exist. existing instruments only in this function" ) )

    let records = ( tickers , values  ) ||> List.map2 ( fun t v -> new MultistrategySchema.Tbl_static_data( InstrumentId = Map.find t instIds, FieldId = fieldId, SourceId = sourceId, Value = v ) )

    updateCore_static_data records




let update_static_data sourceName fieldName sa2FieldName tickers2Descr tickers values =
    
    // full blown update with new insertions in all affected tables when necessary

    let sourceId = 
        
        try
            
            getKey_data_sources ( Set.ofList [ sourceName ] ) 

        with

        | KeysNotFound( name ) -> update_data_sources name ||> List.zip |> Map.ofList

        |> Map.find sourceName
    
    let fieldId = 

        try
            
            getKey_data_fields true ( set [ sa2FieldName ] ) 

        with

        | KeysNotFound( name ) -> [ ( sa2FieldName , fieldName ) ] |> Map.ofList |> update_data_fields ||> List.zip |> Map.ofList

        |> Map.find sa2FieldName
    
    let instIds = 

        try 

            getKey_instruments ( set tickers )

        with

        | KeysNotFound( ids ) -> update_instruments tickers2Descr ||> List.zip |> Map.ofList

    let records = ( tickers , values  ) ||> List.map2 ( fun t v -> new MultistrategySchema.Tbl_static_data( InstrumentId = Map.find t instIds, FieldId = fieldId, SourceId = sourceId, Value = v ) )

    updateCore_static_data records




let private getCore_static_data fieldName isSA2Name tickers =
    
    let multistrategyContext = createMultistrategyContext()

    let sdTable = multistrategyContext.Tbl_static_data

    let instTickers , instIds = 

        try

            tickers |> set |> getKey_instruments |> Map.toList |> List.unzip

        with

        | KeysNotFound ( names ) as e -> raise( e )

    let idInstMap = ( instIds , instTickers ) ||> List.zip |> Map.ofList
    
    let fieldId =

        try 

            [ fieldName ] |> set |> getKey_data_fields isSA2Name |> Map.find fieldName            

        with

        | KeysNotFound( field ) as e -> raise( e )

    query 
    
        {
   
            for row in sdTable do

                select row

        }

        |> Seq.filter ( fun row -> idInstMap.ContainsKey row.InstrumentId && row.FieldId = fieldId ) 
        |> Seq.map ( fun row -> ( fieldName , Map.find row.InstrumentId idInstMap , row.Value ) )
        |> List.ofSeq


    

let get_static_data sa2FieldName tickers =

    // always sa2 name but because it's unique

    let staticData = getCore_static_data sa2FieldName true tickers

    if staticData.Length = 0 then

        Map.add sa2FieldName List.empty Map.empty

    else
      
        partition staticData



let getFundamentalTicker_static_data tickers =

    let staticData = getCore_static_data "fundamentalTicker" true tickers

    if staticData.Length = 0 then

        Map.empty

    else

        let fundId2Ticker = 

            staticData

                |> List.map ( fun ( _ , _ , i ) -> int i )
                |> set
                |> getTicker_instruments

        staticData

            |> List.map ( fun ( _ , n , i ) -> ( n , Map.find ( int i ) fundId2Ticker ) )

            |> Map.ofList
