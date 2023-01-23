module SA2.Common.Table_data_fields

open System
open System.Linq

open DbSchema
open Utils




let update_data_fields ( sa2Names2Fields : Map< string , string > ) =
    
    let multistrategyContext = createMultistrategyContext()

    let sa2Names , _ = sa2Names2Fields |> Map.toList |> List.unzip

    let existingFields =

        query {

                for row in multistrategyContext.Tbl_data_fields do

                select row.SA2Name

        }

    let newNames = sa2Names |> Seq.filter( fun n -> existingFields.Contains n  |> not ) |> List.ofSeq

    if newNames.Length <> 0 then
    
        let ids =

            query {

                for row in multistrategyContext.Tbl_data_fields do

                select row.FieldId

            }

        let maxExistingId = 

            match Seq.isEmpty ids 
            
                with

            | true -> 0 
            | false -> Seq.reduce ( fun i1 i2 -> max i1 i2 ) ids

        let newIds = [ 1 .. newNames.Length ] |> List.map ( fun i -> i + maxExistingId )

        let newRecords = ( newIds, newNames ) ||> List.map2 ( fun i n -> new MultistrategySchema.Tbl_data_fields( FieldId = i , FieldName = Map.find n sa2Names2Fields , SA2Name = n ) )

        multistrategyContext.Tbl_data_fields.InsertAllOnSubmit( newRecords )

        try

            multistrategyContext.SubmitChanges()

            ( newNames , newIds )

        with

        | e -> raise(e)
            
    else

        ( List.empty , List.empty )




let getKey_data_fields isSA2Name ( fieldNames : string Set ) =
    
    let multistrategyContext = createMultistrategyContext()
    
    let fieldsTable = multistrategyContext.Tbl_data_fields

    let fieldIds =  

        match isSA2Name with

        | true -> query {

                            for row in fieldsTable do

                            select ( row.SA2Name , row.FieldId )
                        }

        | false -> query {

                            for row in fieldsTable do

                            select ( row.FieldName , row.FieldId )
                         }

        |> Seq.filter ( fun ( f , _ ) -> fieldNames.Contains f )

        |> Map.ofSeq
    
    let newFields = fieldNames |> Set.filter ( fun n -> fieldIds |> Map.containsKey n |> not )
    
    if ( newFields.Count <> 0 ) then

        raise ( KeysNotFound ( newFields ) )

    else

        fieldIds




let getName_data_fields pickSA2Name ( fieldIds : int Set ) =
    
    let multistrategyContext = createMultistrategyContext()
   
    let fieldsTable = multistrategyContext.Tbl_data_fields

    let fieldNames = 

        match pickSA2Name with

        | true -> query {

                        for row in fieldsTable do

                        select ( row.FieldId , row.SA2Name )
                    }

        | false -> query {

                        for row in fieldsTable do

                        select ( row.FieldId , row.FieldName )
                    }

        

        |> Seq.filter ( fun ( i , _ ) -> fieldIds.Contains i )

        |> Map.ofSeq


    let newIds = fieldIds |> Set.filter ( fun n -> not ( fieldNames |> Map.containsKey n ) )

    if ( newIds.Count <> 0 ) then

        raise ( KeysNotFound ( newIds |> Set.map ( fun i -> i.ToString() ) ) )

    else

        fieldNames





let getMapping_data_fields () =

    let multistrategyContext = createMultistrategyContext()

    query

        { 
        
            for row in multistrategyContext.Tbl_data_fields do

            select ( row.FieldName , row.SA2Name )
            
        }

        |> Seq.toList


