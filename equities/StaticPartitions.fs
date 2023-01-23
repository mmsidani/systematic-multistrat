module SA2.Equity.StaticPartitions




let filterStaticData data instruments =

    let instrumentSet = set instruments

    data

        |> Map.map ( fun k v -> v |> List.filter ( fun ( i , _ ) -> instrumentSet.Contains i ) )




let partitionByStatic ( staticData : Map< string , ( string * string ) list > ) ( fields : string list ) instruments =

    // break the tickers into a list of lists where members share a field. partitioning happens by field, in the order fields were specified in fields

    let filteredStaticData = filterStaticData staticData instruments

    let rec partition ( x : ( string * string ) list ) = 

        let _ , v0 = x.[ 0 ]

        let left , right = x |> List.partition ( fun ( t , v ) -> v = v0 )

        if right.Length <> 0 then

           right |> partition |> List.append [ left |> List.map ( fun ( t , _ ) -> t )  ]

        else

           [ left |> List.map ( fun ( t , _ ) -> t ) ]

    let partitioned = Map.find fields.[ 0 ] filteredStaticData |> partition |> ref

    [

        for i in 1 .. fields.Length - 1 ->
                            
            let fieldMap = Map.find fields.[ i ] filteredStaticData |> Map.ofList
                                   
            partitioned :=

                [ 
    
                    for part in !partitioned ->

                        let partToField = part |> List.map ( fun n -> ( n , Map.find n fieldMap ) )

                        partition partToField 

                ]

                |> List.concat


            !partitioned

    ]

    |> List.concat

    |> ( fun ls -> ( ls , ls |> List.map ( 
    
                                            fun l -> 

                                                let firstName = List.head l

                                                [
                                                    for field in fields ->

                                                        ( field , Map.find field filteredStaticData  |> List.find ( fun ( n , _ ) -> n = firstName ) |> snd ) 

                                                ]

                                                |> Map.ofList

                                        )

                    )

        )




