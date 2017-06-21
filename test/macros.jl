using Feather
using DataUtils
using TheDataMustFlow
using Estuaries
using BenchmarkTools

const filename = "sample.feather"
const batch_size = 1024

src = Feather.Source(filename)

# M = Morphism{Pull}(src)
# @morph M (a::Col{:A}, b::Col{:B}) -> convert(Array, a .+ b)

M = @morphism Pull src (a::Col{:A}, b::Col{:B}) -> convert(Array, a .+ b)

m = morphism(M)
α, = m(1:10)


