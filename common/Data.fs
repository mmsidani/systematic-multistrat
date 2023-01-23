module SA2.Common.Data



open Utils



let lastDataValues x =

    // get the most recent value for every name, whatever the most recent date is

    let names = x |> keySet

    [

    for n in names ->

        Map.find n x

            |> List.sortBy ( fun ( d , _ ) -> -d ) 
            
            |> (
            
                fun l ->
                
                    if List.isEmpty l |> not then
                    
                        Some (

                            List.head l

                                |> ( fun ( _ , v ) -> ( n , v ) )
                            )

                    else

                        None

                )

    ]

    |> List.filter ( fun p -> p.IsSome )
    |> List.map ( fun p -> p.Value )
    |> Map.ofList




let lastDataMap dt x =

    x

        |> Map.map ( fun _ l -> l |> List.filter ( fun ( d , _ ) -> d <= dt ) )

        |> lastDataValues




let selectUniqueDataValues ( a : ( int * string * float) [] ) =

    let uniqueNames = a |> Array.map ( fun ( _ , n , _ ) -> n ) |> set

    [|

    for n in uniqueNames -> 

        let nA = a |> Array.filter ( fun ( _ , name , _ ) -> name = n ) |> Array.sortBy ( fun ( d , _ , _ ) -> d )

        let firstNewValues = nA |> Seq.pairwise |> Seq.toArray |> Array.filter ( fun ( ( _ , _  , v0 ) , ( _ , _ , v1 ) ) -> v1 <> v0 ) |> Array.map ( fun ( _ , ( d , n , v ) ) -> ( d , n , v ) )

        let ( d0 , n0 , v0 ) = Seq.head nA
            
        Array.append [| ( d0 , n0 , v0 ) |] firstNewValues

    |]

    |> Array.concat

