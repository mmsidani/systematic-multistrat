module SA2.Options.BlackScholes

open Options

let bsD1 strike fwd maturity vol =
    ( log ( fwd / strike ) + 0.5 * vol**2.0 * maturity ) / (vol * sqrt maturity)

let bsFwdPrice ( opt : Option ) fwd maturity vol =
    let d1 = bsD1 opt.strike fwd maturity vol
    let d2 = d1 - vol * sqrt maturity
    let normal = MathNet.Numerics.Distributions.Normal ()
    
    // BS option fwd price
    if opt.optType = OptionType.call then
        normal.CumulativeDistribution d1 * fwd - normal.CumulativeDistribution d2 * opt.strike
    else
        normal.CumulativeDistribution -d2 * opt.strike  - normal.CumulativeDistribution -d1 * fwd

let bsPrice opt fwd maturity discountFactor vol =
    discountFactor * ( bsFwdPrice opt fwd maturity vol )

let bsDelta ( opt : Option ) fwd maturity vol =

    if vol < 0. then
        -1.0
    else
        let d1 = bsD1 opt.strike fwd maturity vol
        if opt.optType = OptionType.call then
            ( MathNet.Numerics.Distributions.Normal () ).CumulativeDistribution d1
        else
            ( MathNet.Numerics.Distributions.Normal () ).CumulativeDistribution d1 - 1.0

let bsGamma ( opt : Option ) fwd maturity discountFactor vol =

    if vol < 0. then
        -1.0
    else
        let d1 = bsD1 opt.strike fwd maturity vol
        (MathNet.Numerics.Distributions.Normal() ).Density d1 / ( fwd * discountFactor * vol * sqrt maturity )

let bsVega ( opt : Option ) fwd maturity discountFactor vol  =

    if vol < 0. then
        -1.0
    else
        let d1 = bsD1 opt.strike fwd maturity vol
        (MathNet.Numerics.Distributions.Normal() ).Density d1 * fwd * discountFactor * sqrt maturity
    
let bsTheta ( opt : Option ) fwd maturity discountFactor vol =

    if vol < 0. then

        -1.0

    else

        let d1 = bsD1 opt.strike fwd maturity vol
        let sqtMaturity = sqrt maturity
        let d2 = d1 - vol * sqtMaturity
        let r = - log discountFactor / maturity
        let normal = MathNet.Numerics.Distributions.Normal()

        if opt.optType = OptionType.call then

            discountFactor * ( -fwd * ( normal.Density d1 ) * vol / ( 2.0 * sqtMaturity ) - r * opt.strike * normal.CumulativeDistribution d2 )

        else

            discountFactor * ( -fwd * ( normal.Density d1 ) * vol / ( 2.0 * sqtMaturity ) + r * opt.strike * normal.CumulativeDistribution -d2 )

let bsRho ( opt : Option ) fwd maturity discountFactor vol =

    if vol < 0. then

        -1.0

    else
        
        let d2 = bsD1 opt.strike fwd maturity vol - vol * sqrt maturity

        if opt.optType = OptionType.call then

            discountFactor * opt.strike * maturity * MathNet.Numerics.Distributions.Normal().CumulativeDistribution d2 

        else

            - discountFactor * opt.strike * maturity * MathNet.Numerics.Distributions.Normal().CumulativeDistribution -d2