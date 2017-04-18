
include("../src/utils.jl")

vec = collect(2:30)
bi = batchiter(vec, 3)

for b ∈ bi
    println(b)
end


