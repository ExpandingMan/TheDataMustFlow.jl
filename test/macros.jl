using Feather
using DataUtils
using TheDataMustFlow
using Estuaries
using BenchmarkTools

const filename = "sample.feather"
const batch_size = 1024

src = Feather.Source(filename)

M = Morphism{Pull}(src)
# @morph M (a::Col{:A}, b::Col{:B}) -> convert(Array, a .+ b)

# M = @morphism Pull src (a::Col{:A}, b::Col{:B}) -> convert(Array, a .+ b)

expr = :(@morph M function (a::Col{:A}, b::Col{:B})
             Float32[a b]
         end)

# expr = :(@morph M function (a::Col{:A,UInt16}, b::Col{:B,UInt16})
#     Float64[a b]
# end)

mac = macroexpand(expr)

eval(expr)

m = morphism(M)
α, = m(1:10)


