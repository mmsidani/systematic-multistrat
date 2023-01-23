module SA2.Equity.RelativeShares

type FieldList = string list



open System

open SA2.Common.Utils



let private marketSharesAux ( relShares : Map< string , float > ) nameToGics =

    let names = relShares |> keySet

    names

        |> Set.fold ( 
        
                        fun s n  -> 
                            
                            let g = Map.find n nameToGics 

                            if Map.containsKey g s then

                                Map.add g ( Map.find n relShares :: Map.find g s ) s

                            else

                                Map.add g [ Map.find n relShares ] s

                    ) Map.empty
   



let herfindahlIndex ( relShares : Map< string , float > ) nameToGics =
    
    // this calculates the "normalized" Herfindahl index

    marketSharesAux relShares nameToGics 

        |> Map.map 
        
                ( 
        
                fun k rl -> 
            
                    let sum = List.sum rl
                    rl |> List.map ( fun r -> r / sum ) // scale to sum to 1.0
                
                )
                                      
        |> Map.map 
        
                ( 

                fun g rl -> 
                        
                    rl 
                            
                    |> List.map ( fun r -> r * r )
                       
                    |> ( fun r2l -> 
                    
                                if rl.Length <> 1 then
                            
                                    ( List.sum r2l - 1.0 / float rl.Length ) / ( 1.0 - 1.0 / float rl.Length )

                                else

                                    1.0
                        )
                                                            
                )
                



let marketDominance ( relShares : Map< string , float > ) nameToGics =

    marketSharesAux relShares nameToGics

        |> Map.map ( fun g rl -> rl |>  List.sort |> Seq.pairwise |> Seq.map ( fun ( r1 , r2 ) -> ( r1 - r2 ) ** 2.0 ) |> Seq.sum )
                
    