module SA2.Common.Returns





let calculateReturns levels =
        
    levels 
                        
        |> Seq.sortBy ( fun ( d , _ ) -> d ) 
                            
        |> Seq.pairwise

        |> Seq.toList

        |> List.map ( fun ( ( _ , v0 ) , ( d1 , v1 ) ) -> 
                            
                        if v0 <> 0.0 then
                                            
                            Some ( d1 , v1 / v0 - 1.0 )

                        else

                            None )

        |> List.filter ( fun p -> p.IsSome )

        |> List.map ( fun p -> p.Value )




let calculateReturnsFromMap date2Level =

    date2Level

        |> Map.toList

        |> calculateReturns

        |> Map.ofList