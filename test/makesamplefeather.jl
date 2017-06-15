using DataUtils
using DataTables

const NROWS = 10^6
const filename = "sample.feather"



function sample_dataset(nrows::Integer)
    makecol(n::Real) = (1.0:Float64(nrows)) + n
    DataTable(Header1=1:nrows, Header2=1:nrows,
              A=makecol(0), B=makecol(10), C=makecol(100), D=makecol(1000))
end

function sample_dataset(::Type{Nullable}, nrows::Integer)
    makecol(n::Real) = NullableArray([i for i ∈ (1.0:Float64(nrows)) + n])
    DataTable(Header1=NullableArray(collect(1:nrows)), Header2=NullableArray(collect(1:nrows)),
              A=makecol(0), B=makecol(10), C=makecol(100), D=makecol(1000))
end

function sample_dataset_nulls(nrows::Integer, startidx::Integer=2, intervalidx::Integer=8)
    data = sample_dataset(Nullable, nrows)
    for i ∈ startidx:intervalidx:nrows
        data[i, :A] = Nullable{Float64}()
        data[i, :C] = Nullable{Float64}()
    end
    data
end

function makesample(filename::String, dataset::DataTable)
    featherWrite(filename, dataset, overwrite=true)
end


info("writing feather...")
makesample(filename, sample_dataset(NROWS))
info("done!")

