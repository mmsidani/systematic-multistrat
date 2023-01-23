module SA2.Options.Simulate

open Options
open VarianceReplication
open Control

let oneStepSim optPrices ( portfolios : Map<string, OptionPortfolio > )= 

    portfolios |> Map.fold ( fun s _ v -> 

                                    s + valueOptPortfolio optPrices v

                           )
                            
                           0.0 

let oneStepSimCashflow tcPerContract optPrices0 optPrices1 ( portfolios0 : Map< string , OptionPortfolio > ) ( portfolios1 : Map< string , OptionPortfolio > ) =

    let diff = ( Map.map ( fun k v -> if Map.containsKey k portfolios1 then
                                                                                             
                                                diffOptPortfolios v ( Map.find k portfolios1 )

                                         else

                                                v

                         )  portfolios0

               , portfolios1 )

               ||>  Map.fold ( fun s k v -> if Map.containsKey k s then

                                                s // in intersection so already taken care of above

                                            else

                                                Map.add k ( OptionPortfolio ( v.shares |> Array.map ( fun s -> -s ) , v.options )  ) s

                             )

    let transactCosts = transactionCosts tcPerContract diff
    
    let port0Val0 = portfolios0 |> oneStepSim optPrices0

    let port0Val1 = portfolios0 |> oneStepSim optPrices1

    let port1Val1 = portfolios1 |> oneStepSim optPrices1
    
    // now we want to be in portfolios1

    port0Val1 - port1Val1 - transactCosts 
