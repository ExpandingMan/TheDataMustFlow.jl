__precompile__(true)

module TheDataMustFlow

using DataStreams
using NullableArrays

const DEFAULT_FILTER_BATCH_SIZE = 16384
const DEFAULT_HARVEST_BATCH_SIZE = 16384

#=========================================================================================
    TODO list:
        1. Finish harvester and sower basic implementation.
        2. Figure out what to do about categorical shit.
        3. Figure out DataStreams PR for pulling partial columns.
        4. Handling of nulls.
        5. Figure out batch alignment and truncation.
        6. Vastly improve interface.
        7. Documentation.
        8. Do for time series.
=========================================================================================#

include("abstracts.jl")
include("utils.jl")
include("datastreams_extensions.jl")
include("streamfilter.jl")
include("harvester.jl")

end # module
