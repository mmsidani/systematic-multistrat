module SA2.Equity.IndexWeights


open SA2.Common.DbSchema
open SA2.Common.Utils

open EquityMetrics
  

// MissingDataAlert: if we're missing DatabaseFields.marketCap for an index member than that member is excluded from all further index calculations -- in effect, it's as if that name is not a member of that index
        

let rec capIndexWeights index2Cap indexMembersWeights =

    let rec checkAndScale cap x =

        let larger , smaller = x |> List.partition ( fun ( _ , w ) -> w > cap )

        if larger.Length = 0 then

            x

        else

            let sumSmaller = smaller |> List.sumBy ( fun ( _ , w ) -> w ) 

            let scalingFactor = ( 1.0 - float larger.Length * cap ) / sumSmaller

            smaller

                |> List.map ( fun ( n , w ) -> ( n , w * scalingFactor ) )

                |> checkAndScale cap

                |> List.append ( larger |> List.map ( fun ( n , w ) -> ( n , cap ) ) )


    indexMembersWeights

        |> Map.map ( 
        
                    fun i v -> 

                        if Map.containsKey i index2Cap then

                            let indexCap = Map.find i index2Cap

                            v |> checkAndScale indexCap

                        else

                            v

                    )
          
          

          
let memberWeights date name2FxRate marketData priceWeightedIndexes index2Cap indexMembersData indexes =
  
    let notKnownMembersData =

        indexMembersData
            |> Map.filter ( fun k _ -> Set.contains k indexes )

    let marketCaps = 
    
        getDbDataForDate marketData date DatabaseFields.marketCap

            |> Map.map ( fun n v -> Map.find n name2FxRate * v )

    let prices = getDbDataForDate marketData date DatabaseFields.price

    let index2TotalMarketCap = 
            
        notKnownMembersData 

            |> Map.filter ( fun i _ -> Set.contains i priceWeightedIndexes |> not )
        
            |> Map.map ( fun _ l -> l |> Seq.filter ( fun n -> Map.containsKey n marketCaps ) |> Seq.map ( fun n -> Map.find n marketCaps ) |> Seq.sum )

    let index2TotalPrices =

        notKnownMembersData 

            |> Map.filter ( fun i _ -> Set.contains i priceWeightedIndexes )

            |> Map.map ( fun _ l -> l |> Seq.filter ( fun n -> Map.containsKey n prices ) |> Seq.map ( fun n -> Map.find n prices ) |> Seq.sum )

    notKnownMembersData 
        
        |> Map.map ( fun i l -> 
            
                        if Set.contains i priceWeightedIndexes |> not then

                            let iMCapTotal = Map.find i index2TotalMarketCap
                            
                            l 
                                |> List.filter ( fun n -> Map.containsKey n marketCaps )
                             
                                |> List.map ( fun n -> ( n , Map.find n marketCaps / iMCapTotal ) )

                        else

                            let iPricesTotal = Map.find i index2TotalPrices

                            l 
                            
                                |> List.filter ( fun n -> Map.containsKey n prices )

                                |> List.map ( fun n -> ( n , Map.find n prices / iPricesTotal ) )

                    )

        |> capIndexWeights index2Cap
        