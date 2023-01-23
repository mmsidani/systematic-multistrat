module SA2.Common.Table_equity_universe

open System
open System.Data
open System.Data.Linq
open Microsoft.FSharp.Data.TypeProviders
open Microsoft.FSharp.Linq

open Utils
open Dates
open DbSchema
open Table_instruments
open Table_data_fields




let private getForDate_equity_universe date =
    
    let multistrategyContext = createMultistrategyContext()

    let dbTable = multistrategyContext.Tbl_equity_universe

    let priorUniverses =

        query {
            
            for row in dbTable do
                
            where ( row.Date <= date )
                
            select row

            }

    let dates = 

        query {
        
            for row in priorUniverses do

            select row.Date

            }

    let mostRecentDate =

        match Seq.isEmpty dates with

            | true -> None
            | false -> Seq.reduce ( fun d1 d2 -> max d1 d2 ) dates |> Some

    match mostRecentDate with

        | Some( date ) -> priorUniverses |> Seq.filter ( fun row -> row.Date = date ) |> Seq.map ( fun row -> row.InstrumentId ) |> Set.ofSeq
        | None -> Set.empty




let get_equity_universe date =

    let universeIds = getForDate_equity_universe date

    try

        getTicker_instruments universeIds

            |> Map.toList
            |> List.unzip
            |> snd
            
    with

    | KeysNotFound( s ) as e -> raise( e )




let getUnion_equity_universe () =
    
    let multistrategyContext = createMultistrategyContext()

    query {

        for row in multistrategyContext.Tbl_equity_universe do

        select row.InstrumentId

    }

    |> set

    |> getTicker_instruments

    |> Map.fold ( fun s _ v -> v :: s ) List.empty




let private updateCore_equity_universe criterion instrumentIds equityUniverse =
    
    let multistrategyContext = createMultistrategyContext()

    let records = equityUniverse |> List.map ( fun ( d , n ) -> new MultistrategySchema.Tbl_equity_universe( InstrumentId = Map.find n instrumentIds, Date = date2Obj d , Criterion = criterion ) ) 
    
    let existingRecords =

        query {

            for row in multistrategyContext.Tbl_equity_universe do

            select ( ( row.InstrumentId ,  row.Date |> obj2Date ) , row )

        }

        |> Map.ofSeq

    for record in records do

        let recordKey = ( record.InstrumentId ,  record.Date |> obj2Date )

        match Map.containsKey recordKey existingRecords 
                                  
            with

        | true -> let er = Map.find recordKey existingRecords

                  er.Criterion <- record.Criterion // if for some odd unforeseen reason we want to update the criterion

        | false -> multistrategyContext.Tbl_equity_universe.InsertOnSubmit( record )

    try
        
        multistrategyContext.SubmitChanges()

    with

    | e -> raise( e )




let update_equity_universe criterion names2Descr ( equityUniverse : ( int * string ) list ) =

    let allNames = equityUniverse |> List.fold ( fun s ( d , n ) -> Set.add n s ) Set.empty
    
    let instrumentIds =

        try

             getKey_instruments allNames

        with

        | KeysNotFound( s ) -> update_instruments names2Descr |> ignore ; getKey_instruments allNames
    
    updateCore_equity_universe criterion instrumentIds equityUniverse




let updateExisting_equity_universe criterion ( equityUniverse : ( int * string ) list ) =

    let allNames = equityUniverse |> List.fold ( fun s ( d , n ) -> Set.add n s ) Set.empty
    
    let instrumentIds =

        try

             getKey_instruments allNames

        with

        | KeysNotFound( names ) -> raise ( Exception ( "these names: " + names.ToString() + " don't exist. existing names only in this function" ) )
    
    updateCore_equity_universe criterion instrumentIds equityUniverse