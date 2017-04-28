using DataTables
using DataStreams
using Feather
using TheDataMustFlow
using DataUtils
using BenchmarkTools

const filename = "sample.feather"
const batch_size = 1024

src = Feather.Source(filename)
src_sch = Data.schema(src)
nrows = size(src, 1)


f(i) = (i % 2 == 0)
g(i) = (i % 3 == 0)
function rawfilt(idx::AbstractVector{<:Integer})
    v1 = TheDataMustFlow.streamfrom(src, Data.Column, NullableVector{Int}, idx, 1)
    v2 = TheDataMustFlow.streamfrom(src, Data.Column, NullableVector{Int}, idx, 2)
    find(f.(v1) .& f.(v2)) + idx[1] - 1
end

sfilter = streamfilter(src, Header1=f, Header2=g)


idx = 1:100

@info benchraw = @benchmark rawfilt(idx)

@info bench = @benchmark sfilter(idx)

# FUCKING AWESOME !!!!!


