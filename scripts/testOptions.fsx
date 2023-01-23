#r@"..\3rdparty\Microsoft.Solver.Foundation.dll"
#r@"..\3rdparty\MathNet.Numerics.dll"
#r@"..\sa2-dlls\common.dll"
#r@"..\sa2-dlls\options.dll"



#load ".\settingsProduction.fsx"


open System
open System.IO

open SA2.Options

open SA2.Common

open Utils
open Math
open Io
open Options
open BlackScholes
open Options
open VarianceReplication
open Control
open SimStats
open Simulate

let exactFwdDiscCalculation ( optPrices : Map<string, float> ) ( opts : (Option * Option) [] )  =

    printfn "%A" opts

    if opts.Length <> 2 then
        raise ( ["wrong number of options"] |> concatFileInfo __LINE__ |> VarianceException )

    let c0 = Map.find (fst opts.[0]).optName optPrices
    let p0 = Map.find (snd opts.[0]).optName optPrices
    let c1 = Map.find (fst opts.[1]).optName optPrices
    let p1 = Map.find (snd opts.[1]).optName optPrices
    let k0 = (fst opts.[0]).strike
    let k1 = (fst opts.[1]).strike

    let fwd = ( k1 * (c0 - p0) - k0 * ( c1 - p1) ) / ( (c0 - p0) - ( c1 - p1) )
    let disc = (c0 - p0) / ( fwd - k0 )
    let disc2 = (c1 - p1) / ( fwd - k1 )

    printfn "fwd, disc: %g , %g, %g" fwd disc disc2

let testBS () =

    let c = Option(30.0, "07/17/2014", OptionType.call,ExerciseType.european,"junk","opt")
    let p = Option(30.0, "07/17/2014", OptionType.put,ExerciseType.european,"junk","opt")
    let mat = 1.0
    let r = 0.01
    let fwd = 25.0 * exp( r * mat)
    let disc = exp( -( r * mat ))
    let vol = 0.2

    printfn "%g , %g, %g, %g, %g,%g" ( bsPrice c fwd mat disc vol ) ( bsDelta c fwd mat vol ) (bsGamma c fwd mat disc vol ) ( bsVega c fwd mat disc vol ) ( bsTheta c fwd mat disc vol ) ( bsRho c fwd mat disc vol )
    printfn "%g , %g, %g, %g, %g,%g" ( bsPrice p fwd mat disc vol ) ( bsDelta p fwd mat vol ) (bsGamma p fwd mat disc vol ) ( bsVega p fwd mat disc vol ) ( bsTheta p fwd mat disc vol ) ( bsRho p fwd mat disc vol )
    
    let optPrice = bsPrice c fwd mat disc vol

    printfn "vol: %g" ( optVol 1.0e-10 bsPrice c optPrice fwd mat disc )
    printfn "vol: %g" ( optVol 1.0e-10 bsPrice p optPrice fwd mat disc )

