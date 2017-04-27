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

f(x, y) = x .- y
g(x, y, z) = x .+ y .- z
h(x, y, α, β) = x .+ y .- α .- β


M = Morphism{PullBack}(src, [(3,4), (3,4,5), (3,4,5,6)], [f, g, h])

m = morphism(M)


x, y, z = m(40:60)

