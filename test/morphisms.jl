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

M = Morphism{Pull}(src, [(3,4), (3,4,5), (3,4,5,6)], [f, g, h])

m = morphism(M)

x, y, z = m(1:10)
=#

# create a sink to put data into
dtypes = [DataType[eltype(dt) for dt ∈ Data.types(src)]; Float32; Float32]
header = [Symbol.(Data.header(src)); :γ; :δ]
sink = DataTable(dtypes, header, nrows)


h₁(x) = [x+1, x+2]
h₂(x) = [x+3, x+4]
M = Morphism{Push}(sink, [(1,2), (3,4)], [h₁, h₂])

m! = morphism(M)

m!(1:3, ones(Int,3), 2ones(Float64,3))


