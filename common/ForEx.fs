module SA2.Common.ForEx

open Utils
open Data



let formCurrencyPairsDivisors baseCurrency currencies =

    // bloomberg does not have rates for GBpUSD, etc.; we handle it here

    let ret = 
    
        currencies 
        
            |> List.filter ( fun ( c : string ) -> c <> baseCurrency ) 
            
            |> List.map ( 
            
                        fun c -> 
            
                            let cUpper = c.ToUpper()
                            
                            if cUpper <> c then 
                            
                                ( c , cUpper + baseCurrency , 100.0) 

                            else

                                ( c , c + baseCurrency , 1.0 )
                                
                         ) 

    if Seq.exists ( fun c -> c = baseCurrency ) currencies then

        ( baseCurrency , baseCurrency , 1.0 ) :: ret

    else 

        ret

    |> List.map ( fun ( c , p , d ) -> ( ( c , p ) , ( c , d ) ) )

    |> List.unzip

    |> ( fun ( cp , cd ) -> ( Map.ofList cp , Map.ofList cd ) )




let name2ForexRate forexIndicator baseCurrency forexData nameToCurrency date =
    
    let currencies = valueSet nameToCurrency |> Set.toList
    
    let ccyPairs , ccyDivisors = formCurrencyPairsDivisors baseCurrency currencies
    
    let dataForDate = forexData |> lastDataMap date
    
    nameToCurrency
        |> ( fun junk -> junk |> Map.iter ( fun _ v -> if (Map.containsKey v ccyPairs |> not) || (Map.containsKey v ccyDivisors |> not)
                                                            || (Map.containsKey ( Map.find v ccyPairs + " " + forexIndicator ) dataForDate |> not) then
                                                            printfn "%s is missing info or" v 
                                                            printfn "%s is missing data" ( Map.find v ccyPairs + " " + forexIndicator ))
                         junk )

        |> Map.map ( fun _ v -> Map.find ( Map.find v ccyPairs + " " + forexIndicator ) dataForDate / Map.find v ccyDivisors )




let formCurrencyPairs forexIndicator baseCurrency currencies =
    
    let ccyPairs , _ = formCurrencyPairsDivisors baseCurrency currencies

    ccyPairs

        |> Map.toList
        |> List.unzip
        |> snd
        |> set
        |> Set.toList
        |> List.map ( fun cc -> cc + " " + forexIndicator )




let convertToPerspective baseCcy conversionData date = 

    conversionData

        |>  lastDataMap date

        |> ( fun m -> 

                let baseFactor = 1.0 + Map.find baseCcy m

                m |> Map.map ( fun _ v -> baseFactor / ( 1.0 + v ) ) 
            )