module SA2.Options.SimStats

open Options
open BlackScholes

let portfolioDelta fwd maturity vols ( optPort : OptionPortfolio ) = 
    
    Array.fold2 ( fun s w o -> s + w * bsDelta o fwd maturity ( Map.find o.optName vols ) ) 0.0 optPort.shares optPort.options

let portfolioGamma fwd maturity discountFactor vols ( optPort : OptionPortfolio ) = 
    
    Array.fold2 ( fun s w o -> s + w * bsGamma o fwd maturity discountFactor ( Map.find o.optName vols ) ) 0.0 optPort.shares optPort.options

let portfolioVega fwd maturity discountFactor vols ( optPort : OptionPortfolio ) = 
    
    Array.fold2 ( fun s w o -> s + w * bsVega o fwd maturity discountFactor ( Map.find o.optName vols ) ) 0.0 optPort.shares optPort.options

let portfolioTheta fwd maturity discountFactor vols ( optPort : OptionPortfolio ) =
    
    Array.fold2 ( fun s w o -> s + w * bsTheta o fwd maturity discountFactor ( Map.find o.optName vols ) ) 0.0 optPort.shares optPort.options
