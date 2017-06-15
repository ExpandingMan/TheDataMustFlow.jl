__precompile__(true)

module TheDataMustFlow

using DataStreams
using NullableArrays
using MacroTools

import Base.convert
import Base.identity

const DEFAULT_BATCH_SIZE = 16384
const DEFAULT_FILTER_BATCH_SIZE = 2^18
const DEFAULT_HARVEST_BATCH_SIZE = 16384
const DEFAULT_SOW_BATCH_SIZE = 16384

#=========================================================================================
    TODO list:
    √  -1. Implement generic Morphism object and make everything descend from it.
    √   0. Switch to using Tasks in anticipation of parallelization.
    √   1. Finish harvester and sower basic implementation.
    √   -. Sower *must* be able to both insert into existing and extract to new.
    √   -. Implement source and sink interfaces for Harvester and Sower.
        2. Figure out what to do about categorical shit.
        3. Figure out DataStreams PR for pulling partial columns.
    √   4. Handling of nulls.
        5. Figure out batch alignment and truncation.
        6. Allow FilterStreams to handle arbitrarily complicated constraints.
        7. Vastly improve interface.
        8. Documentation.
        9. Do for time series.
        10. Consider making all morphisms single-function, but with methods
            for Vector{<:AbstractMorphism}.
=========================================================================================#

include("abstracts.jl")
include("utils.jl")
include("datastreams_extensions.jl")
include("morphism.jl")
include("streamfilter.jl")
include("harvester.jl")
include("sower.jl")
include("migrator.jl")
include("datastreams.jl")


end # module
