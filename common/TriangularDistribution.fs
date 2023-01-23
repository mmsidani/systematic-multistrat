module SA2.Common.TriangularDistribution

open System

open MathNet.Numerics

let triangularSample l m u n =

    // l lower ; m mode ; u upper ; n number of rands desired

    if not ( l <= m && m <= u && l <> u && n >= 0 ) then
        
        raise( Exception( "ERROR in TriangularSample(): illegal input. we should have: a <= c <= b and n >= 0. the passed args are:" + l.ToString() +  u.ToString() + m.ToString() + n.ToString() ) )

    if n = 0 then
    
        List.empty

    else

        let mT = new Random.MersenneTwister()

        let us = List.init n ( fun i -> mT.NextDouble() )

        let firstDenom = ( u - l ) * ( m - l )

        let secondDenom = ( u - l ) * ( u - m )

        [
        
            for i in 0 .. n - 1 -> 

                    if us.[ i ] <= ( m - l ) / ( u - l ) then

                        l + sqrt( us.[ i ] * firstDenom )

                    else

                        u - sqrt( ( 1.0 - us.[ i ] ) * secondDenom )

        ]
