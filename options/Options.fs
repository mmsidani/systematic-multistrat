module SA2.Options.Options

open Microsoft.SolverFoundation.Common


open System


open SA2.Common.Utils
open SA2.Common.Math


let optVolMaxIters = 100


type OptionType =
    | put = 0
    | call = 1




type ExerciseType = 
    | european = 0
    | american = 1




type Option(s : float, m : string, t : OptionType, e : ExerciseType, u : string, n : string ) =

    member this.strike =s
    member this.maturityDate = m
    member this.optType = t
    member this.exercise = e
    member this.underlying = u
    member this.optName = n

    override this.Equals( o  ) = 

        match o 
        
            with

                | :? Option as y -> Unchecked.equals this.optName y.optName
                | _ -> raise ( ["wrong type"] |> concatFileInfo __LINE__ |> VarianceException )

    override this.GetHashCode() = Unchecked.hash this.optName

    override this.ToString () = this.optName + ", type: " + this.optType.ToString()




type OptionPortfolio ( shares : float[] , opts : Option[] ) =
    
    let mutable matDate0 = ""
    let mutable underlying0 = ""
    let mutable ( sharesOpts : float[] * Option[] ) = ( Array.empty , Array.empty )
    
    do 
        if opts.Length <> 0 then

            matDate0 <- opts.[0].maturityDate
            underlying0 <- opts.[0].underlying
    
        if not ( opts  |> Array.forall ( fun o -> o.maturityDate = matDate0 && o.underlying = underlying0 ) && shares.Length = opts.Length ) then

            raise( [ "OptionPortfolio is only defined for options with the same expiry date and on the same underlying"] |> concatFileInfo __LINE__ |> VarianceException )

        sharesOpts <- ( shares , opts ) ||> Array.zip |> Array.sortBy ( fun ( s , o ) -> o.strike ) |> Array.unzip

    member this.shares = fst sharesOpts
    member this.options = snd sharesOpts
    member this.underlying = underlying0
    member this.maturityDate = matDate0

    new ( wo : float[] * Option[]  ) = OptionPortfolio( fst wo, snd wo)




let parseOptionMaturity mat =

    DateTime.ParseExact( mat , "MM/dd/yy" , null )




let optVol tol ( optModel : Option -> float -> float -> float -> float -> float ) ( opt : Option) optPrice fwd maturity discountFactor =

    let strike = opt.strike
    let optType = opt.optType

    let mutable sigmaLow = 0.001
    let mutable sigmaHigh = 1.0
    let f x = optModel opt fwd maturity discountFactor x - optPrice
    
    let mutable counter = 0

    while f sigmaLow > 0. && counter < optVolMaxIters do

        sigmaLow <- sigmaLow / 10.0
        counter <- counter + 1

    if counter >= optVolMaxIters 
    
        && f sigmaLow > 0. then

        -1.0
    else 

        counter <- 0

        while f sigmaHigh < 0. && counter < optVolMaxIters do

            sigmaHigh <- sigmaHigh * 1.5

        if counter >= optVolMaxIters && ( f sigmaLow > 0. ) then

            -1.0

        else

            bisec f sigmaLow sigmaHigh tol
   



let rec partitionOptions ( optionFieldFun : Option  -> 'a  ) ( opts : Option[] ) =

    if opts.Length = 0 then

        raise ( ["array is empty" ] |> concatFileInfo __LINE__ |> VarianceException )

    let field0 = optionFieldFun opts.[0]

    let left , right = opts |> Array.partition ( fun o -> optionFieldFun o = field0  )

    let partition = [| ( field0,  left ) |]  |> Map.ofArray

    if right.Length <> 0 then

        right |> partitionOptions optionFieldFun |> Map.fold (fun s k v -> Map.add k v s ) partition 

    else

        partition




let partitionByType ( opts : Option[] ) =

    opts |> Array.partition ( fun x -> x.optType = OptionType.call )




let partitionPortfolioByType ( opts : OptionPortfolio ) =

    let portPartition = Array.zip opts.shares opts.options |> Array.partition ( fun wo -> ( snd wo ).optType = OptionType.call ) 
    
    ( OptionPortfolio ( fst portPartition |> Array.unzip ) , OptionPortfolio ( snd  portPartition |> Array.unzip ) )




let formStraddles ( opts : Option [] ) =

    // check parameters

    if opts.Length = 0 then

        raise ( Exception( "array is empty" ) )

    let und0 = opts.[ 0 ].underlying
    let mat0 = opts.[ 0 ].maturityDate

    if 
    
        opts |> Array.forall ( fun o -> o.underlying = und0 && o.maturityDate = mat0 ) |> not 
    
        then

        raise ( Exception( "not all options have same maturity and underlying" ) )

    let allCalls , allPuts = opts |> partitionByType

    let callStrikes = allCalls |> Array.map ( fun x -> x.strike ) |> Set.ofArray
    let putStrikes = allPuts |> Array.map ( fun x -> x.strike ) |> Set.ofArray
    let commonStrikes = Set.intersect callStrikes putStrikes

    if commonStrikes.IsEmpty then

        raise ( [ "there are no straddles" ] |> concatFileInfo __LINE__ |> VarianceException )

    let calls = allCalls |> Array.filter (fun o -> commonStrikes.Contains o.strike ) |> Array.sortBy ( fun o  -> o.strike )
    let puts = allPuts |> Array.filter (fun o -> commonStrikes.Contains o.strike ) |> Array.sortBy ( fun o -> o.strike )

    ( calls , puts ) ||> Array.zip




let findAtmBox ( optPrices : Map<string, float> ) ( straddles : (Option * Option) [] ) =

    // straddles are assumed to have been sorted by strike prior to calling this

    let index = 
    
        straddles 
        
            |> Array.tryFindIndex ( fun ( c , p ) -> Map.find c.optName optPrices < Map.find p.optName optPrices  )

            |> ( 
            
                fun i -> if i.IsSome then
                            
                            i.Value

                          else

                            raise( Exception ( "Cannot determine ATM box" ) )

                )

    if index = 0 then
        
        raise ( Exception( "ATM is not bracketed" ) )

    straddles.[ index - 1 .. index ]




let calcFwdDiscount ( optPrices : Map<string, float> ) ( optUndMaturities : Map<string, float> ) ( underlyingPrices : Map<string, float> ) ( opts : (Option * Option) [] ) =
    
    if opts.Length = 0 then

        raise ( ["array is empty" ] |> concatFileInfo __LINE__ |> VarianceException )

    let mat0 = Map.find ( fst opts.[0] ).underlying optUndMaturities

    if 
    
        opts.Length < 2 
    
            ||  opts |> Array.forall ( fun ( c , p ) -> Map.find c.underlying optUndMaturities = mat0 && Map.find p.underlying optUndMaturities = mat0 && c.strike = p.strike ) |> not 
            
        then

            raise ( Exception( "fwdPrice requires at least 2 pairs of options and that all options in the array have the same maturity and that pairs have the same strike" ) )

    let undPrice = Map.find (fst opts.[0]).underlying underlyingPrices

    let objective = System.Func< float[] , _ > ( fun pts -> 

                                                    let optErr = opts 
                                                                    |> Array.fold ( 
                                                                    
                                                                                    fun acc ( c , p ) -> acc + ( Map.find c.optName optPrices - Map.find p.optName optPrices 
                                                                                                                    - pts.[1] * ( pts.[0] - c.strike ) ) ** 2.0
                                                                                    ) 0.0

                                                    optErr + ( undPrice / pts.[1] - pts.[0] ) ** 2.0 |> sqrt
                                                )

    nonLinearSolver objective [| undPrice ; 1.0 |] [| 0.0 ; 0.0 |] [| Rational.op_Explicit Rational.PositiveInfinity ; Rational.op_Explicit Rational.PositiveInfinity |]
    



let optionFromBBTicker exercise optionExpiry optStrings =

    try
    
        optStrings 
       
            |> Array.map ( 
            
                        fun ( os : string ) ->

                            let subs = os.Split ( [|' '|] )

                            let optType = 
                            
                                match subs.[2].Substring( 0, 1 ) 
                                
                                    with

                                        | "C" -> OptionType.call

                                        | _ as t -> if t <> "P" then 

                                                        raise ( ["unknown option type"] |> concatFileInfo __LINE__ |> VarianceException )

                                                    else

                                                        OptionType.put

                            let strike = float ( subs.[2].Substring( 1 ) )

                            Option(strike, subs.[1], optType, exercise, subs.[0], os )
                                                   
                        ) 

    with

        | VarianceException( x ) as e -> raise ( e )

        | _ as e -> raise( [ e.ToString()  ] |> concatFileInfo __LINE__ |> VarianceException )




let setITMVol ( opts : Option [] ) vols =

    let optsByUnds = opts |> partitionOptions ( fun o -> o.underlying ) 

    let straddles = optsByUnds |> Map.fold ( fun s _ v -> formStraddles v |> Array.append s ) Array.empty

    let ret = straddles |> Array.fold ( fun m ( c , p ) -> 

                                            let cV = Map.tryFind c.optName vols 
                                            let pV = Map.tryFind p.optName vols

                                            if not pV.IsNone && not cV.IsNone then

                                                if cV.Value = -1.0  then

                                                    Map.add c.optName pV.Value m // whatever that is. if -1.0, we haven't lost anything ( nor gained )

                                                elif pV.Value = -1.0 then

                                                    Map.add p.optName cV.Value m // whatever that is. if -1.0, we haven't lost anything ( nor gained )

                                                else

                                                    m

                                            else

                                                m
                            ) 
                            
                            vols

    ret




let valueOptPortfolio optPrices ( ots : OptionPortfolio ) =

    Array.fold2 ( fun s w ( o : Option ) -> s + w * Map.find o.optName optPrices  ) 0.0 ots.shares ots.options  




let diffOptPortfolios ( optPort0 : OptionPortfolio ) ( optPort1 : OptionPortfolio ) = 
    
    if not (Array.isEmpty optPort0.options) && not (Array.isEmpty optPort1.options)  && not ( optPort0.underlying = optPort1.underlying && optPort0.maturityDate = optPort1.maturityDate ) then

        raise ( [ "portfolios must have same underlying and maturity to be diffed" ] |> concatFileInfo __LINE__ |> VarianceException )

    // Note: the weight of the option is quite prescribed. it changes only when the weight of the underlying in the index changes. we're pre-empting this by doing the difference all the time
    let opts0 = ( optPort0.shares, optPort0.options ) ||> Array.map2 ( fun w o -> o.optName , ( w , o ) ) |> Map.ofArray
    let opts1 = ( optPort1.shares, optPort1.options ) ||> Array.map2 ( fun w o -> o.optName , ( w , o ) ) |> Map.ofArray

    let ret = opts0 |> Map.fold ( fun s _ v -> 

                                        let o = ( snd v ).optName

                                        if Map.containsKey o opts1 then

                                            Array.append s [| ( ( opts1 |> Map.find o |> fst ) - ( fst v ) , snd v ) |]

                                        else

                                            Array.append s [| ( - fst v, snd v ) |]
                                )

                                Array.empty  

    OptionPortfolio (

                opts1 |> Map.fold ( fun s _ v -> 

                                            let o = ( snd v ).optName

                                            if Map.containsKey o opts0 then

                                                s // in intersection already taken care of above

                                            else

                                                Array.append s [| ( fst v, snd v ) |]

                                  )

                                  ret

                      |> Array.unzip

                    )