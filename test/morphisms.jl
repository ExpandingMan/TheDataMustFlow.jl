using Feather
using DataUtils
using TheDataMustFlow
using Estuaries
using BenchmarkTools


const filename = "sample.feather"
const batch_szie = 1024

src = Feather.Source(filename)
nrows = size(src,1)
E = Estuaries.Source(src)

#=
f(x, y) = x .- y
g(x, y, z) = x .+ y .- z
h(x, y, α, β) = x .+ y .- α .- β

M = Morphism{PullBack}(src, [(3,4), (3,4,5), (3,4,5,6)], [f, g, h])

m = morphism(M)

x, y, z = m(40:60)
=#


# create a sink to put data into
dtypes = [DataType[eltype(dt) for dt ∈ Data.types(src)]; Float32; Float32]
header = [Symbol.(Data.header(src)); :γ; :δ]
sink = DataTable(dtypes, header, nrows)


f(x) = x + 1000.0
M = Morphism{PushForward}(sink, Tuple[(3,5)], Function[f])

m! = morphism(M)

m!(2:21, [1:20 21:40])


