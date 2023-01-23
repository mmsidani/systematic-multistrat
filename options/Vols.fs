module SA2.Options.Vols


open System

open SA2.Common.Dates
open SA2.Common.Utils

open Options
open OptionWeights


let private unitsInYearConstant = 365.0 // why units? because i want to be able to switch from days to minutes if i have to

let private numberOfNearestMaturitiesConstant = 2

let private unitsTo30DaysConstant = 30.0 // we do it in days. precise vix calculation requires minutes




let private calculateUnitsToMaturity ( forDate : DateTime ) maturity =

    DateTime.op_Subtraction( maturity , forDate ).TotalDays




let private calculateYearsToMaturity ( forDate : DateTime ) maturity =
    
     calculateUnitsToMaturity forDate maturity / unitsInYearConstant
    
    
    

let private calculateNearestMaturities ( timeToMaturityCalculator : DateTime -> float ) forDateObj maturityDates =

    maturityDates

        |> set

        |> Set.map ( fun m -> parseOptionMaturity m )

        |> Set.filter ( fun m -> m >= forDateObj )

        |> Set.toArray

        |> Array.sort

        |> ( 
            
            fun maturities -> 

                if maturities.Length >= numberOfNearestMaturitiesConstant then

                    maturities.[ 0 .. numberOfNearestMaturitiesConstant - 1 ]

                elif maturities.Length < numberOfNearestMaturitiesConstant then

                    maturities.[ 0 .. maturities.Length - 1 ]

                else

                    raise( Exception( "no suitable maturities" ) )

            )

        |> Array.map ( fun m -> timeToMaturityCalculator m )




let private averageBidAsk bidPrices askPrices =

    bidPrices

        |> Map.map 
            ( 
        
            fun optName b -> 

                let a = Map.tryFind optName askPrices

                if a.IsSome then Some( ( b + a.Value ) * 0.5 ) else None 
            )

        |> Map.filter ( fun _ v -> v.IsSome )

        |> Map.map ( fun _ v -> v.Value )




let private filterZeroBid optBidPrices opts =

    let pairedOpts =

        opts

            |> Seq.sortBy ( fun ( o : Option ) -> o.strike )
            |> Seq.map
                (

                fun o ->
                    let v = Map.tryFind o.optName optBidPrices
                    if v.IsSome && v.Value <> 0.0 then Some( o ) else None

                )

            |> Seq.pairwise
            |> Array.ofSeq

    let indexNone = pairedOpts |> Array.tryFindIndex ( fun ( o1 , o2 ) ->  o1.IsNone && o2.IsNone )

    if indexNone.IsSome && indexNone.Value <> 0 then

        pairedOpts.[ 0 .. indexNone.Value - 1 ]

            |> Array.map ( fun ( o1 , o2 ) -> o1.Value )

    else

        Array.empty
        


      
let calculateIndexValue forDate optBidPrices optAskPrices underlyingPrices ( opts : Option [ ] ) =

    let underlying = opts.[ 0 ].underlying

    if opts |> Array.forall ( fun o -> o.underlying = underlying ) |> not

        then

            raise ( Exception ( "options passed here should be on one underlying only" ) )

    
    let optPrices = averageBidAsk optBidPrices optAskPrices

    let forDateObj = forDate |> date2Obj

    let nearestMaturities = // number of nearest set in a private constant above

        opts

            |> Array.map ( fun o -> o.maturityDate )

            |> calculateNearestMaturities ( calculateYearsToMaturity forDateObj ) forDateObj

    let maturity2Opts = 
    
        opts 
        
            |> partitionOptions ( fun o -> o.maturityDate )

            |> Map.filter ( fun d _ -> nearestMaturities |> Array.exists ( fun m -> m = ( d |> parseOptionMaturity |> calculateYearsToMaturity forDateObj ) ) )

    let maturityStrings = maturity2Opts |> keySet

    [|

    for maturity in maturityStrings ->

        let matObj = maturity |> parseOptionMaturity
        let matFloat = matObj |> calculateYearsToMaturity forDateObj
        let matOpts = Map.find maturity maturity2Opts

        let matStraddles = formStraddles matOpts
        let matAtmBox = findAtmBox optPrices matStraddles
        let matUndMaturities = [ ( underlying , matFloat ) ] |> Map.ofList
        let matFwd , matDisc = calcFwdDiscount optPrices matUndMaturities underlyingPrices matAtmBox

        let cutOffStrike = matAtmBox.[ 0 ] |> fst |> ( fun o -> o.strike )

        let otmPuts = 
            matOpts 
                |> Array.filter ( fun o -> o.optType = OptionType.put && o.strike < cutOffStrike )
                |> filterZeroBid optBidPrices
                
        let otmCalls = 
            matOpts 
                |> Array.filter ( fun o -> o.optType = OptionType.call && o.strike > cutOffStrike )
                |> filterZeroBid optBidPrices

        let optionsAtCutOff = matOpts |> Array.filter ( fun o -> o.strike = cutOffStrike )

        let deltaK = matOpts |> Array.map ( fun o -> o.strike ) |> averageAreaMethod

        let optionTerm ( o : Option ) =

            ( Map.find o.strike deltaK  / o.strike ** 2.0 ) * ( Map.find o.optName optPrices )

        (
            ( otmPuts |> Array.fold ( fun s o -> s + optionTerm o ) 0.0 )

            +

            ( otmCalls |> Array.fold ( fun s o -> s + optionTerm o ) 0.0 )

            +
            
            ( optionsAtCutOff |> Array.fold ( fun s o -> s + 0.5 * optionTerm o ) 0.0 ) // at cut-off, prices of call and put are averaged

        )

        |> ( fun v -> ( matFloat , calculateUnitsToMaturity forDateObj matObj , v * 2.0 / matFloat / matDisc - ( matFwd / cutOffStrike - 1.0 ) ** 2.0 / matFloat ) )
                                
    |]

    |> ( 
        
        fun a -> 
    
            if a.Length <> 2 then
            
                raise ( Exception( "logic only implemented for exactly 2 option maturities" ) ) 

            let ( m0 , u0 , v0 ) = a.[ 0 ]
            let ( m1 , u1 , v1 ) = a.[ 1 ]

            if not ( u1 >= unitsTo30DaysConstant && unitsTo30DaysConstant >= u0 ) then

                raise ( Exception( "maturities don't bracket 30 days." ) ) 

            let ret =
            
                ( m0 * v0 * ( u1 - unitsTo30DaysConstant ) / ( u1 - u0 ) + m1 * v1 * ( unitsTo30DaysConstant - u0 ) / ( u1 - u0 ) ) * unitsInYearConstant / unitsTo30DaysConstant 
            
                    |> sqrt

            ret * 100.0

        )

