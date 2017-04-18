__precompile__(true)

module TheDataMustFlow

using DataStreams
using NullableArrays

import Base.filter

const DEFAULT_FILTER_BATCH_SIZE = 16384

include("abstracts.jl")
include("utils.jl")
include("datastreams_extensions.jl")
include("streamfilter.jl")
include("harvester.jl")

end # module
