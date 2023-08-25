#include "MatchImpl.h"
#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"

namespace DB
{
namespace
{
struct NameTokenLike
{
    static constexpr auto name = "tokenLike";
};

using TokenLikeImpl = MatchImpl<NameTokenLike, /*SQL LIKE */ true, /*revert*/false>;
using FunctionTokenLike = FunctionsStringSearch<TokenLikeImpl>;

}

void registerFunctionTokenLike(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTokenLike>();
}

}
