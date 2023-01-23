module SA2.Options.VarianceReplication

open System.IO


open SA2.Common.Utils
open SA2.Common.Math

open Options
open OptionWeights


type DispersionType =
    | long = 0
    | short = 1
  



let replicateVariance step fwd maturity ( opts : Option[] ) =

    let underlying0 = opts.[0].underlying
    let matDate0 = opts.[0].maturityDate

    if opts.Length < 4 || not ( opts |> Array.forall ( fun o -> o.underlying = underlying0 ) ) || not ( opts |> Array.forall ( fun o -> o.maturityDate = matDate0 ) ) then

        raise ( ["too few options or options on more than 1 underlying or options don't have the same maturity"] |> concatFileInfo __LINE__ |> VarianceException )

    // largest strike below fwd
    let cutOffStrike = opts |> Array.map (fun o -> o.strike ) |> Array.fold ( fun s k -> if k < fwd then
                                                                                            max s k 
                                                                                         else 
                                                                                            s 
                                                                            ) -infinity

    // use OTM options, whose strikes fall within a range
    let optsToUse = opts |> Array.filter ( fun o -> 

                                                o.strike <= fwd * ( 1.0 + step ) && o.strike >= fwd * ( 1.0 - step )
                                                    && match o.optType with
                                                            | OptionType.call -> o.strike > fwd || o.strike = cutOffStrike
                                                            | _ -> o.strike < fwd

                                         )

    let strike2Step = optsToUse |> Array.map ( fun o -> o.strike ) |> averageAreaMethod

    // Note: scale by fwd
    OptionPortfolio ( optsToUse |> Array.map ( fun o -> 

                                                    let weight = Map.find o.strike strike2Step * 2.0 / maturity / o.strike ** 2.0

                                                    if o.strike <> cutOffStrike then
                                                        ( weight , o )
                                                    else
                                                        // at the cut-off strike we take the average call-put price, the net effect of which is to halve the weight of each
                                                        ( weight / 2.0 , o )
                                                            
                                              )

                                |> Array.unzip )




let replicateVariances step fwds optMaturities ( opts : Option[] ) =

    let optsByUnderlying = opts |> partitionOptions ( fun o -> o.underlying )

    optsByUnderlying |> Map.map ( fun u v -> replicateVariance step ( Map.find u fwds ) (Map.find u optMaturities) v )




let fairVariance optPrices shortRate fwd maturity ( optsPort : OptionPortfolio ) =

    let underlying = optsPort.options.[0].underlying

    try

        let portVal = optsPort |> valueOptPortfolio optPrices

        let cutOffStrike = optsPort.options |> Array.filter ( fun o -> o.optType = OptionType.put ) |> Array.fold ( fun s o -> max s o.strike ) -infinity

        // the fair variance. Note: the scaling by the forward price does not change this value because fwd will have been canceled out in the sum just done
        // Note2: the discount factor implied by the options ATM prices is of no use here because it includes dividend yield
        portVal * exp ( shortRate * maturity ) - ( fwd / cutOffStrike - 1.0 ) ** 2.0 / maturity

    with

        | _ as e -> raise ( [ e.ToString() ] |> concatFileInfo __LINE__ |> VarianceException )




let impliedCorrelation basketName underlyingWeights optPrices shortRates fwds optMaturities ( optsPort : Map< string, OptionPortfolio > ) = 

    let fairVariances = optsPort |> Map.map ( fun k v -> fairVariance optPrices ( Map.find k shortRates ) (Map.find k fwds ) ( Map.find k optMaturities ) v ) 

    Map.find basketName fairVariances / ( fairVariances |> Map.filter ( fun k _ -> k <> basketName ) |> Map.fold ( fun s k v -> s + v * (Map.find k underlyingWeights)**2.0 ) 0.0 )





let basketCovariance dispersionType step basketName optMaturities fwds underlyingWeights opts =
    
    let basketUnderlyings = underlyingWeights |> Map.toArray |> Array.unzip |> fst

    let optsPortMap = replicateVariances step fwds optMaturities opts
    
    let longShort = match dispersionType with

                        | DispersionType.long -> 1.0 // long dispersion is short covariance
                        | _ -> -1.0

    optsPortMap |> Map.map ( fun k v -> if k <> basketName then

                                            OptionPortfolio( v.shares |> Array.map ( fun w -> longShort * ( Map.find k underlyingWeights )**2.0 * w ) , v.options )

                                        else

                                            OptionPortfolio( v.shares |> Array.map ( fun w -> -longShort * w ) , v.options )
                           )




let premiumNeutralBeta basketName optPrices ( portfolios : Map< string, OptionPortfolio > )  =

    let valuePort = valueOptPortfolio optPrices
    let totalValueUndPorts = portfolios |> Map.fold (fun st k v -> if k <> basketName then

                                                                        st + valuePort v
                                                                   else
                                                                        st ) 
                                                    0.0 

    let valueBasketPort = Map.find basketName portfolios |> valuePort
    
    valueBasketPort / totalValueUndPorts |> abs



    
let longCovPremNeut step basketName optPrices optMaturities fwds underlyingWeights opts =
   
    let optsPortMap = basketCovariance DispersionType.short step basketName optMaturities fwds underlyingWeights opts

    let ratio = premiumNeutralBeta basketName optPrices optsPortMap

    // short underlying (long covariance) and make it premium neutral
    optsPortMap |> Map.map ( fun k v -> if k <> basketName then

                                            OptionPortfolio( v.shares |> Array.map ( fun w -> ratio * w ) , v.options )

                                        else

                                            v
                           )




let longCovVegaNeut step basketName underlyingWeights optPrices shortRates fwds optMaturities opts = 
   
    let optsPorts = basketCovariance DispersionType.short step basketName optMaturities fwds underlyingWeights opts

    let implCorr = impliedCorrelation basketName underlyingWeights optPrices shortRates fwds optMaturities optsPorts

    // short underlying (long covariance) and make it premium neutral
    optsPorts |> Map.map ( fun k v -> if k <> basketName then

                                            OptionPortfolio( v.shares |> Array.map ( fun w -> implCorr * w ) , v.options )

                                        else

                                            v
                         )