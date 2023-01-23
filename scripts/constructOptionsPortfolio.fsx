#r@"..\3rdparty\Microsoft.Solver.Foundation.dll"
#r@"..\3rdparty\MathNet.Numerics.dll"
#r@"..\sa2-dlls\common.dll"
#r@"..\sa2-dlls\options.dll"


#load ".\settingsProduction.fsx"


open System
open System.IO

open SA2

open Common.Utils
open Common.Math
open Common.Io
open Options.BlackScholes
open Options.Options
open Options.VarianceReplication
open Options.Control
open Options.SimStats
open Options.Simulate


let specialTreatment optionExpiry (od : seq< string > ) =

    od |> Seq.map ( fun s -> 
                            let subs = s.Split( [| ',' |] )

                            if subs.[1].StartsWith "ID" then

                                let subSubs = subs.[1].Split( [|' '|] )
                                let l = subSubs.[0].Length
                                let t = subSubs.[0].Substring( l-1, 1 )
                                let n = "IBEX" // subSubs.[0].Substring(0, l-1)
                                subs.[0] + "," + n + " " + optionExpiry + " " + t + subSubs.[subSubs.Length-1] + "," + subs.[2]

                            else

                                s
                  )

let runCovTrade ( osw : StreamWriter) daysInYear step basketName forDate shortRates optPrices optVolumes optOpints underlyingWeights underlyingPrices ( opts : Option[] ) =

    try

        let forDateDate = DateTime.ParseExact(forDate, "yyyyMMdd", null)
        // Note: yes, pairs get overwritten in the next statement. that's OK, we need one, any one of them for a given underlying
        let underlying2MaturityDate = opts |> Array.map ( fun o ->  ( o.underlying , o.maturityDate ) ) |> Map.ofArray  
        if not ( opts |> Array.forall ( fun o -> o.maturityDate = Map.find o.underlying underlying2MaturityDate ) ) then
            raise ( [ "options on one underlying on a given day should have the same maturity date" ] |> concatFileInfo __LINE__ |> VarianceException )

        let underlying2Maturity = underlying2MaturityDate |> Map.map ( fun _ v -> DateTime.op_Subtraction( DateTime.ParseExact( v, "MM/dd/yy", null), forDateDate ).TotalDays / daysInYear )
        let underlying2Opts = opts |> partitionOptions ( fun o -> o.underlying )

        let fwdsDiscs = underlying2Opts |> Map.map ( fun _ v -> 
                                                    let atmBox = v |> formStraddles |> findAtmBox optPrices
                                                    calcFwdDiscount optPrices underlying2Maturity underlyingPrices atmBox
                                              )
      
        let fwds = fwdsDiscs |> Map.map (fun k v -> fst v )
        let discs = fwdsDiscs |> Map.map (fun k v -> snd v )

        // get premium neutral long covariance trade
        let covTrade = basketCovariance DispersionType.short step basketName underlying2Maturity fwds underlyingWeights opts
        
         // this replicates computation done under the covers in the previous statement. TODO
        let und2VarPort = replicateVariances step fwds underlying2Maturity opts
       
        let beta = 1.0 // impliedCorrelation basketName underlyingWeights optPrices shortRates fwds underlying2Maturity und2VarPort // premiumNeutralBeta basketName optPrices covTrade
        
        let covOutput = covTrade |> Map.map ( fun k v -> if k <> basketName then
                                                                OptionPortfolio( v.shares |> Array.map ( fun w -> beta * w ) , v.options )
                                                         else
                                                                v
                                            )
       
        osw.Write( ",,,,,,," + forDate  + "\r" )
        
        let volu optName = 
            try
                let oVolume = optVolumes |> Map.tryFind optName
                match oVolume with
                                    | Some( x ) -> float (x)
                                    | _ -> 0.
            with
            | _ as t -> raise ( [ t.ToString() ] |> concatFileInfo __LINE__ |> VarianceException )

        let opu optName = 
            try
                let oOpenInt = optOpints |> Map.tryFind optName
                match oOpenInt with
                                        | Some( x ) -> float (x)
                                        | _ -> 0.
            with
            | _ as t -> raise ( [ t.ToString() ] |> concatFileInfo __LINE__ |> VarianceException )

        let optTextWriter ( sw : StreamWriter ) ( w : float) ( p : float) ( o : Option )  = 

            let optName = o.optName
            let vu = volu optName
            let ou = opu optName
            sw.Write( optName + "," + w.ToString() + "," + p.ToString() + "," + vu.ToString() + "," + ou.ToString() + "\r" )
            
        // write the options portfolios on the underlyings
        let doWrite ( v : OptionPortfolio ) = Array.iter2 ( fun w ( o : Option ) -> optTextWriter osw w ( Map.find o.optName optPrices ) o ) v.shares v.options                   
        covOutput |> Map.iter ( fun k v -> 
                                        if k <> basketName then
                                            doWrite v
                              )

        // ... and the basket at the bottom
        Map.find basketName covOutput |> doWrite

        ( covOutput, underlying2Maturity, und2VarPort, fwds, discs ) 

    with

         | VarianceException( x ) as e -> raise( e )
         | _ as t -> raise ( [ t.ToString() ] |> concatFileInfo __LINE__ |> VarianceException )

let runSimulation ( psw : StreamWriter ) basketName undWeights histData shortRates optMaturities fwds discountFactors vols date und2VarPort covTrade = 
    
    try 

        let afterDateData = histData |> Array.filter ( fun pair -> fst pair >= date )

        let portValues = afterDateData |> Array.map ( fun pair -> ( fst pair, oneStepSim ( snd pair ) covTrade )   )
        let basketNameValues = afterDateData |> Array.map ( fun pair -> ( fst pair, oneStepSim ( snd pair ) ( covTrade |> Map.filter ( fun k _ -> k = basketName ) ) ) )

        let implCorrs = afterDateData 
                            |> Array.map ( fun pair -> ( fst pair, impliedCorrelation basketName undWeights ( snd pair ) shortRates fwds optMaturities und2VarPort ) ) 

        psw.Write( ",,,,,,," + date.ToString() + "\r" )
        
        Array.iter ( fun ( v , r, b ) -> psw.Write( (fst v).ToString() + "," + (snd v).ToString() + "," + (snd r).ToString() + "," + (snd b).ToString() + "\r" ) ) ( Array.zip3 portValues implCorrs basketNameValues )

        ( portValues , implCorrs )

    with

    | VarianceException( x ) as e -> raise ( e )
    | _ as t -> raise ( [ t.ToString() ] |> concatFileInfo __LINE__ |> VarianceException )

let runPortfolio ( rsw : StreamWriter ) tcPerContract histData date portfolios0 portfolios1 =

    try
 
        let upToData = histData |> Array.filter ( fun pair -> fst pair <= date ) |> Array.unzip |> snd

        let ret = oneStepSimCashflow tcPerContract upToData.[ upToData.Length - 2 ] upToData.[ upToData.Length - 1 ] portfolios0 portfolios1

        let allNames = ( Map.fold (fun s k _ -> Set.add k s  ) Set.empty portfolios0 , portfolios1 ) ||> Map.fold (fun s k _ -> Set.add k s  )

        let txt =  Set.fold ( fun s k -> 

                                    let port0 = if Map.containsKey k portfolios0 then
                                                    portfolios0 |> Map.filter ( fun n _ -> n = k )
                                                else
                                                    Map.ofArray [| ( k , OptionPortfolio( Array.empty , Array.empty ) ) |]

                                    let port1 = if Map.containsKey k portfolios1 then
                                                    portfolios1 |> Map.filter ( fun n _ -> n = k )
                                                else
                                                    Map.ofArray [| ( k , OptionPortfolio( Array.empty , Array.empty ) ) |]

                                    let simStep = oneStepSimCashflow tcPerContract upToData.[ upToData.Length - 2 ] upToData.[ upToData.Length - 1 ]  port0 port1 

                                    s + "," + k + "," + simStep.ToString() 
                                    
                             ) "" allNames

        rsw.Write( date.ToString() + "," + ret.ToString() + "," + txt + "\r" )

    with

    | VarianceException( x ) as e -> raise ( e )
    | _ as t -> raise ( [ t.ToString() ] |> concatFileInfo __LINE__ |> VarianceException )
        

let runDollarGamma ( dsw : StreamWriter ) date optPrices underlyingPrices fwds optMaturities discountFactors vols und2VarPorts =
    
    try

        let dollarGammas = und2VarPorts
                            |> Map.toArray
                            |> Array.map ( fun pair -> 
                                                let und2VarPort = snd pair
                                                let d = fst pair
                                                ( d , und2VarPort |> Map.map ( fun k v -> 

                                                                                    let fwd = Map.find k fwds
                                                                                    let mat = Map.find k optMaturities
                                                                                    let disc = Map.find k discountFactors
                                                                                    let und = Map.find k underlyingPrices

                                                                                    portfolioGamma fwd mat disc vols v * 0.5 * und ** 2.0 ) 

                                                                  |> Map.toArray
                                                ) 
                                         )

        dollarGammas |> Array.iter ( fun x -> dsw.Write( date.ToString() + "," + ( fst x ).ToString() + "," + ( writeTupleArray ( snd x ) ) + "\r" ) )

        dollarGammas

    with

    | VarianceException( x ) as e -> raise ( e )
    | _ as t -> raise ( [ t.ToString() ] |> concatFileInfo __LINE__ |> VarianceException )

let runCash ( csw : StreamWriter ) basketName undWeights date undPrices =
    
    try
        let text = Map.fold ( fun s k _ ->  s + "," + k + "," + ( Map.find k undPrices).ToString() + "," + ( Map.find k undWeights ).ToString() ) "" undWeights


        csw.Write( date.ToString() + "," + basketName + "," + ( Map.find basketName undPrices ).ToString() + "," + text + "\r" )

    with

    | VarianceException( x ) as e -> raise ( e )
    | _ as t -> raise ( [ t.ToString() ] |> concatFileInfo __LINE__ |> VarianceException )

let runOpts ( gsw : StreamWriter ) date undPrices optPrices fwds maturities discountFactors vols ( covTrade : Map< string, OptionPortfolio > ) =

    covTrade |> Map.iter ( fun k v ->
     
                                let callPuts = partitionPortfolioByType v

                                let value = valueOptPortfolio optPrices
                                let cVal = fst callPuts |> value
                                let pVal = snd callPuts |> value

                                let delta = portfolioDelta ( Map.find k fwds ) ( Map.find k maturities ) vols
                                let cDelta =  fst callPuts |> delta
                                let pDelta = snd callPuts |> delta

                                let gamma = portfolioGamma ( Map.find k fwds ) ( Map.find k maturities ) ( Map.find k discountFactors ) vols
                                let cGamma = fst callPuts |> gamma
                                let pGamma = snd callPuts |> gamma

                                let theta = portfolioTheta ( Map.find k fwds ) ( Map.find k maturities ) ( Map.find k discountFactors ) vols
                                let cTheta = fst callPuts |> theta
                                let pTheta = snd callPuts |> theta

                                let vega = portfolioVega ( Map.find k fwds ) ( Map.find k maturities ) ( Map.find k discountFactors ) vols
                                let cVega = fst callPuts |> vega
                                let pVega = snd callPuts |> vega
  
                                let undPrice = Map.find k undPrices

                                gsw.Write( date.ToString() + "," + k + "," + undPrice.ToString() + "," + cVal.ToString() + "," + pVal.ToString() + "," + cDelta.ToString() + "," + pDelta.ToString() + "," + cGamma.ToString() + "," + pGamma.ToString() + "," 
                                                + cVega.ToString() + "," + pVega.ToString() + "," + cTheta.ToString() + "," + pTheta.ToString() + "," + ( 0.5 * ( cGamma + pGamma ) * undPrice**2.0 ).ToString() + "\r" )

                        )

let runCov ( vsw : StreamWriter ) rollingWindow simDate undWeights ( prices : Map< int, Map< string, float > > ) =

    let names = undWeights |> Map.toArray |> Array.unzip |> fst
        
    let pricesArray = prices |> Map.toArray |> Array.sortBy ( fun v -> fst v ) |> Array.rev |> Array.unzip
    let histMs = pricesArray |> snd
    
    let numNames , numHist = names.Length , histMs.Length
    
    let undPriceTable = Array2D.init numHist numNames ( fun i j -> Map.find names.[j] histMs.[i] )
    let weights = Array.init numNames ( fun i -> Map.find names.[i] undWeights ) // to make sure names are in the same order as undPriceTable
    
    let undRets = Array2D.mapi ( fun i j p -> log ( p / undPriceTable.[ i + 1, j ] ) ) undPriceTable.[ 0 .. numHist - 2 , 0 .. numNames - 1 ]
    
    // Note: so it's the responsibility of the calling function to make sure enough data was passed for the rolling window to work
    let dates = pricesArray |> fst |> Array.filter ( fun d -> d >= simDate ) |> Array.rev
    let covs = Array.fold ( fun s i -> Array.append s [| (
                                                            let covMat = covariance undRets.[ i .. i + rollingWindow - 1 , 0 .. numNames - 1 ]
                                                            avgCov weights covMat
                                                      ) |]
                          ) Array.empty [| 0 .. dates.Length - 1 |]
               |> Array.rev


    Array.iter2 ( fun d c -> vsw.Write( d.ToString() + "," + c.ToString() + "\r" ) ) dates covs

    Array.map2 ( fun d c -> ( d, c ) ) dates covs |> Map.ofArray
      
let run () =

    // bindings defined in the try block are not in scope in the with block. hence the following acrobatics with nested try blocks
    try

        let osw = new StreamWriter("C:/Users/Majed/devel/InOut/optionPortfolios.csv")
        let psw = new StreamWriter("C:/Users/Majed/devel/InOut/portfolioSim.csv")
        let csw = new StreamWriter("C:/Users/Majed/devel/InOut/cashSim.csv")
        let gsw = new StreamWriter("C:/Users/Majed/devel/InOut/portPartsSim.csv")
        let vsw = new StreamWriter("C:/Users/Majed/devel/InOut/covSim.csv")
        let dsw = new StreamWriter("C:/Users/Majed/devel/InOut/dgammaSim.csv")
        let rsw = new StreamWriter("C:/Users/Majed/devel/InOut/retsSim.csv")

        try

            let tol = 1.0e-10
            let daysInYear = 365.0
            let optionExpiry = "07/19/13"
            let covarianceRollingWindow = 20
            let tcPerContract = 0.0 ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 0.5
            let step = 0.05

            let underlyingPrices = "c:/Users/Majed/devel/InOut/2013-07-17.rawIndexData.csv" |> readFile |> parseOptionData
            let optPrices = "c:/Users/Majed/devel/InOut/2013-07-17.rawsx5eoptionsData.csv" |> readFile |> specialTreatment optionExpiry |> parseOptionData
            let volOpint = "c:/Users/Majed/devel/InOut/2013-07-17.rawsx5eoptionsOpIntData.csv" |> readFile |> specialTreatment optionExpiry 
            let optVolumes = volOpint |> Seq.filter ( fun s -> not ( s.Contains ".openint" ) ) |> Seq.map ( fun s -> s.Replace( ".volume", "" ) ) |> parseOptionData
            let optOpints = volOpint |> Seq.filter ( fun s -> not ( s.Contains ".volume" ) ) |> Seq.map ( fun s -> s.Replace( ".openint", "" ) ) |> parseOptionData

            let simStartDate = 20130430
            let basketName = "SX5E"
            let contractSizes = [| ("SX5E", 10.0) ; ("CAC", 10.0) ; ("DAX", 5.0) ; ("FTSEMIB", 2.5) ; ("AEX" , 100.0); ("IBEX" , 1.0) |] |> Map.ofArray
            // TODO should be dynamic
            let undWeightsRaw = [| ("CAC", 0.3757) ; ("DAX" , 0.2943) ; ("FTSEMIB", 0.0742) ; ("AEX" , 0.0638) ; ("IBEX" , 0.1238) |] 
            // bogus
            let shortRates = [| ("SX5E", 0.0) ; ("CAC", 0.0) ; ("DAX", 0.0) ; ("FTSEMIB", 0.0 ) ; ("AEX" , 0.0); ("IBEX" , 0.0) |] |> Map.ofArray
       
            let dates = optPrices |> Map.toArray |> Array.unzip |> fst |> Array.filter ( fun d -> d >= simStartDate )
            
            // scale weights to sum to 1
            let sumWeights = undWeightsRaw |> Array.unzip |> snd |> Array.sum
            let undWeights = undWeightsRaw |> Array.map ( fun w -> ( fst w , ( snd w ) / sumWeights ) ) |> Map.ofArray
            let histData = ( optPrices |> Map.toArray |> Array.sortBy ( fun pair -> fst pair ) )

            let oneDayTrade = runCovTrade osw daysInYear step basketName 
            let oneDaySim = runSimulation psw basketName undWeights histData
            let oneDayRet = runPortfolio rsw tcPerContract histData
            let oneDayCash = runCash csw basketName undWeights
            let oneDayOpts = runOpts gsw

            let mutable und2VarPorts = Map.empty
                        
            runCov ( vsw : StreamWriter ) covarianceRollingWindow simStartDate undWeights underlyingPrices |> ignore

            let mutable portfolios0 = Map.empty

            for forDate in dates do

                printfn "now doing: %d" forDate

                let forDateOptPrices = Map.find forDate optPrices
                let forDateOptVolumes = Map.find forDate optVolumes
                let forDateOptOpints = Map.find forDate optOpints
                let forDateUndPrices = Map.find forDate underlyingPrices
                let forDateOpts = optionFromBBTicker ExerciseType.european optionExpiry ( Map.toArray forDateOptPrices |> Array.unzip |> fst )
        
                let covTrade , optMaturities , und2VarPort , fwds , discs = 
                
                    oneDayTrade ( forDate.ToString() ) shortRates forDateOptPrices forDateOptVolumes forDateOptOpints undWeights forDateUndPrices forDateOpts
                
                let vols = forDateOpts |> Array.fold ( fun s o -> 
                                                            let und = o.underlying
                                                            Map.add o.optName ( optVol tol bsPrice o ( Map.find o.optName forDateOptPrices ) (Map.find und fwds ) ( Map.find und optMaturities) ( Map.find und discs) ) s
                                                     )  Map.empty

                let allVols = setITMVol forDateOpts vols
                Map.iter ( fun k v -> if v = -1.0 then
                                            printfn "didn't work with: %s" k
                         ) allVols

                oneDaySim shortRates optMaturities fwds discs vols forDate und2VarPort covTrade |> ignore

                oneDayRet forDate portfolios0 covTrade 
                portfolios0 <- covTrade

                oneDayCash forDate forDateUndPrices
                oneDayOpts forDate forDateUndPrices forDateOptPrices fwds optMaturities discs vols covTrade 

                und2VarPorts <- Map.add forDate und2VarPort und2VarPorts

                runDollarGamma ( dsw : StreamWriter ) forDate forDateOptPrices forDateUndPrices fwds optMaturities discs vols und2VarPorts |> ignore

            // flatten at end of simulation
            oneDayRet dates.[dates.Length-1] portfolios0 Map.empty

            osw.Close()
            psw.Close()
            csw.Close()
            gsw.Close()
            vsw.Close()
            dsw.Close()
            rsw.Close()

        with

        | VarianceException( x ) -> osw.Close(); psw.Close(); csw.Close(); gsw.Close(); vsw.Close(); dsw.Close(); rsw.Close(); printfn "%s" x
        | _ as e ->  osw.Close(); psw.Close(); csw.Close(); gsw.Close(); vsw.Close(); dsw.Close(); rsw.Close(); printfn "%s" ( e.ToString() )

    with

    | _ as e ->  printfn "at line %s: %s" __LINE__ ( e.ToString() )


run ()
