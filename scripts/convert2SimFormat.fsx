#r@"..\sa2-dlls\common.dll"



#load ".\settingsProduction.fsx"


open System

open SA2.Common.Io
open SA2.Common.Utils


let private instrumentNamesMap =

    [| 
        ( "GACGB10 INDEX" , "au.gg.10y" ) ;
        (  "GCAN10YR INDEX" , "ca.gg.10y" ) ;
        (  "GSWISS10 INDEX" , "sz.gg.10y" ) ;
        (  "GSPG10YR INDEX" , "sp.gg.10y" ) ;
        (  "GDBR10 INDEX" , "ge.gg.10y" ) ;
        (  "GFRN10 INDEX" , "fr.gg.10y" ) ;
        (  "GIDN10YR INDEX" , "id.gg.10y" ) ;
        (  "GIND10YR INDEX" , "in.gg.10y" ) ;
        (  "GBTPGR10 INDEX" , "it.gg.10y" ) ;
        (  "GJGC10 INDEX" , "jp.gg.10y" ) ;
        (  "GVSK10YR INDEX" , "ko.gg.10y" ) ;
        (  "GMXN10YR INDEX" , "mx.gg.10y" ) ;
        (  "GNTH10YR INDEX" , "nl.gg.10y" ) ;
        (  "POGB10YR INDEX" , "pl.gg.10y" ) ;
        (  "MICXRU10 INDEX" , "ru.gg.10y" ) ;
        (  "GSGB10YR INDEX" , "sw.gg.10y" ) ;
        (  "GUKG10 INDEX" , "uk.gg.10y" ) ;
        (  "USGG10YR INDEX" , "us.gg.10y" ) ;
        (  "GSAB9YR INDEX" , "sa.gg.10y" ) ;
        (  "DAX INDEX" ,  "ge.equity" ) ;
        (  "CAC INDEX" ,  "fr.cac40" ) ;
        (  "UKX INDEX" ,  "uk.equity" ) ;
        (  "OMX INDEX" ,  "sw.omx30" ) ;
        (  "SXXP INDEX" ,  "eu.stxe600" ) ;
        (  "TPX INDEX" ,  "jp.equity" ) ;
        (  "AS51 INDEX" ,  "au.asx200" ) ;
        (  "SIMSCI INDEX" , "sg.2mscifree" ) ; 
        (  "SPTSX60 INDEX" , "ca.equity" ) ;
        (  "HSI INDEX" ,  "hk.equity" ) ;
        (  "AEX INDEX" , "nl.equity" ) ;
        (  "IBEX INDEX" , "sp.ibex35" ) ;
        (  "FTSEMIB INDEX" , "it.mib40" ) ;
        (  "OBXP INDEX" , "no.equity" ) ;
        (  "SMI INDEX" , "sz.equity" ) ;
        (  "TAMSCI INDEX" , "tw.2mscifree" ) ;
        (  "SX5E INDEX" , "eu.estx50" ) ;
        (  "INDU INDEX" , "us.dji" ) ;
        (  "NKY INDEX" , "jp.n225" ) ;
        (  "SPX INDEX" , "us.equity" ) ; 
        (  "NDX INDEX" , "us.nasdaq100" ) ;
        (  "KOSPI2 INDEX" , "ko.kospi200" ) ;
        (  "RTY INDEX" , "us.russell2000" ) ;            
        (  "NIFTY INDEX" , "in.nifty" ) ; 
        (  "SET50 INDEX" ,  "th.set50" ) ;
        (  "FBMKLCI INDEX" , "my.equity" ) ;
        (  "WIG20 INDEX" , "pl.wig20" ) ;
        (  "TOP40 INDEX" , "sa.equity" ) ;
        (  "HSCEI INDEX" ,  "hk.chinaent" ) ;
        (  "RTSI$ INDEX" ,  "ru.equity" ) ; 
        (  "XIN9I INDEX" , "ch.a50" ) ; 
        (  "MEXBOL INDEX" , "mx.equity" ) ; 
        (  "IBOV INDEX" , "bz.equity" ) ; 
        (  "XU030 INDEX" , "tk.ise30" ) ; 
        (  "SHSN300 INDEX" , "ch.equity" ) ;

    |]

    |> Map.ofArray


let private baseCountryCode = "us"
let private cashNameSuffix = ".gg.3m"


let private convertDate date =

    let dateString = date.ToString()

    dateString.Substring( 0 , 4 ) + "-" + dateString.Substring( 4 , 2 ) + "-" + dateString.Substring( 6 , 2 )




let convertFormat mapNames portfolioFileName ( outputFile : string ) = 

    let bogusPortRet , bogusPortRisk = 0.0 , 0.0

    let portfolioLines = 
        readFile portfolioFileName
            |> Seq.toArray

    let instruments = 
        portfolioLines.[ 0 ].Split( [| ',' |] ) 
            |> ( fun a -> 
                    if a.[ 0 ] <> "Date" then raise( Exception( "wrong portfolio file format" ) )
                    a )
            |> Array.toList |> List.tail |> List.toArray // first element is "Date"

    let simNames = 
        if mapNames then
            instruments |> Array.map ( fun i -> Map.find i instrumentNamesMap ) 
        else
            instruments

    let baseCashName = baseCountryCode + cashNameSuffix
    let instrument2CashName = 
        instruments 
            |> Array.mapi ( fun i n -> 
                                let simName = simNames.[ i ]
                                ( n , simName.Substring( 0 , 2 ) + cashNameSuffix ) )
            |> Map.ofArray

    let nonBaseCashNames =
        instrument2CashName
            |> valueSet
            |> Set.filter ( fun c -> c <> baseCashName )

    let createFreshCashMap () =

        instruments |> Array.map ( fun i -> ( i , ( Map.find i instrument2CashName , 0.0) ) ) |> Map.ofArray

    let convertedPortfolio =

        [|
            for i in 1 .. portfolioLines.Length - 1 -> 

                let line = portfolioLines.[ i ]

                let elements = line.Split( [| ',' |] )

                let date = convertDate elements.[ 0 ] // int -> yyyy-mm-dd

                let weights = elements.[ 1 .. elements.Length-1 ] |> Array.map ( fun w -> float w )

                let mutable instrument2CashNamesAndWeights = createFreshCashMap ()
                for j in 0 .. weights.Length-1 do
                    let cashName , cashWeight = Map.find instruments.[ j ] instrument2CashNamesAndWeights 
                    instrument2CashNamesAndWeights <- 
                        if cashName <> baseCashName then
                            Map.add instruments.[ j ] ( cashName , cashWeight - weights.[ j ] ) instrument2CashNamesAndWeights
                        else
                            instrument2CashNamesAndWeights

                let nonBaseCashWeights = 
                    instrument2CashNamesAndWeights 
                        |> Map.fold ( fun s i ( c , w ) -> 
                                        if Map.containsKey c s then Map.add c ( Map.find c s + w ) s
                                        else Map.add c w s ) Map.empty 
                        |> Map.filter ( fun k _ -> k <> baseCashName)

                let baseAssetsTotalWeight = 
                    weights
                        |> Array.mapi ( fun i w -> if Map.find instruments.[ i ] instrument2CashName = baseCashName then w else 0.0 ) |> Array.sum
                    
                let baseCashWeight = 1.0 - baseAssetsTotalWeight - ( nonBaseCashWeights |> Map.fold ( fun s _ w -> s - w ) 0.0 )
                let mutable txt = ""
                for nonBaseCashName in nonBaseCashNames do
                    txt <- txt + "," + ( Map.find nonBaseCashName nonBaseCashWeights ).ToString()

                let newLine = 
                    date + "," + bogusPortRet.ToString() + "," + bogusPortRisk.ToString() + "," + baseCashWeight.ToString() + "," 
                    + ( elements.[ 1 .. elements.Length-1 ] |> Array.reduce ( fun e0 e1 -> e0 + "," + e1 ) ) + txt

                newLine
                
        |]

    let newHeader = "Date,portRet,portRisk," + baseCashName + "," + Array.reduce ( fun s0 s1 -> s0 + "," + s1) simNames + "," 
                        + ( nonBaseCashNames |> Set.toArray |> Array.reduce ( fun c0 c1 -> c0 + "," + c1 ) )

    let sw = new IO.StreamWriter( outputFile )

    convertedPortfolio

        |> Array.append [| newHeader |]
        |> Array.iter ( fun l -> sw.WriteLine( l ) ) 

    sw.Close ()



// execute

convertFormat false "//Terra/Users/Majed/AppData/Local/output/ratesStrategyOutput.csv" "//Terra/Users/Majed/devel/InOut/rates.csv"
//convertFormat true "//Terra/Users/Majed/AppData/Local/output/indexesStrategyOutput.csv" "//Terra/Users/Majed/devel/InOut/indexes.csv"
//convertFormat true "//Terra/Users/Majed/devel/staging/output/developedPortfolio.csv" "//Terra/Users/Majed/devel/InOut/developed.csv"
//convertFormat true "//Terra/Users/Majed/devel/InOut/minVar.csv" "//Terra/Users/Majed/devel/InOut/minVarSim.csv"
//convertFormat false "//Terra/Users/Majed/devel/InOut/bestWorst5.csv" "//Terra/Users/Majed/devel/InOut/bestWorst5Sim.csv"
