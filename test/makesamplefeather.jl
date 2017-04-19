using DataUtils
using DataTables

const NROWS = 10^6
const filename = "sample.feather"

function sample_dataset(nrows::Integer)
    makecol(n::Real) = (1.0:Float64(nrows)) + n
    DataTable(Header1=1:nrows, Header2=1:nrows,
              A=makecol(0), B=makecol(10), C=makecol(100), D=makecol(1000))
end

function makesample(filename::String; nrows::Integer=NROWS)
    src = sample_dataset(nrows)
    featherWrite(filename, src, overwrite=true)
end


# makesample(filename)

