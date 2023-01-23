module SA2.Common.Table_data_sources


open System
open System.Linq

open DbSchema
open Utils



let update_data_sources ( sources : string Set ) =
    
    let multistrategyContext = createMultistrategyContext()

    let existingSources =

        query {

                for row in multistrategyContext.Tbl_data_sources do
                
                select row.SourceName

        }

    let newSources = sources |> Seq.filter( fun n -> not ( existingSources.Contains n  ) ) |> List.ofSeq

    if newSources.Length <> 0 then
    
        let ids = 

            query {

                for row in multistrategyContext.Tbl_data_sources do

                select row.SourceId

            }

        let maxExistingId = 

            match Seq.isEmpty ids with
            | true -> 0
            | false -> Seq.reduce ( fun i1 i2 -> max i1 i2 ) ids

        let newIds = [ 1 .. newSources.Length ] |> List.map ( fun i -> i + maxExistingId )
        
        let newRecords = ( newIds, newSources ) ||> List.map2 ( fun i n -> new MultistrategySchema.Tbl_data_sources( SourceId = i , SourceName = n ) )

        multistrategyContext.Tbl_data_sources.InsertAllOnSubmit( newRecords )

        try

            multistrategyContext.SubmitChanges()

            ( newSources , newIds )

        with

        | e -> raise(e)
            
    else

        ( List.empty , List.empty )




let getKey_data_sources ( sourceNames : string Set ) =
    
    let multistrategyContext = createMultistrategyContext()
   
    let sourcesTable = multistrategyContext.Tbl_data_sources

    let sourceIds = 

        query {

            for row in sourcesTable do

            select ( row.SourceName , row.SourceId )
        }

        |> Seq.filter ( fun ( n , _ ) -> sourceNames.Contains n )

        |> Map.ofSeq


    let newSources = sourceNames |> Set.filter ( fun n -> not ( sourceIds |> Map.containsKey n ) )

    if ( newSources.Count <> 0 ) then

        raise ( KeysNotFound ( newSources ) )

    else

        sourceIds




let getName_data_sources ( sourceIds : int Set ) =
    
    let multistrategyContext = createMultistrategyContext()
   
    let sourcesTable = multistrategyContext.Tbl_data_sources

    let sourceNames = 

        query {

            for row in sourcesTable do

            select ( row.SourceId , row.SourceName )
        }

        |> Seq.filter ( fun ( i , _ ) -> sourceIds.Contains i )

        |> Map.ofSeq


    let newIds = sourceIds |> Set.filter ( fun n -> not ( sourceNames |> Map.containsKey n ) )

    if ( newIds.Count <> 0 ) then

        raise ( KeysNotFound ( newIds |> Set.map ( fun i -> i.ToString() ) ) )

    else

        sourceIds