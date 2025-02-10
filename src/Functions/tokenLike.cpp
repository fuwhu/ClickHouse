#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSearch.h>
#include <Functions/MatchImpl.h>

namespace DB
{
namespace
{
struct NameTokenLike
{
    static constexpr auto name = "tokenLike";
};

using TokenLikeImpl = MatchImpl<NameTokenLike, MatchTraits::Syntax::Like, MatchTraits::Case::Sensitive, MatchTraits::Result::DontNegate>;
using FunctionTokenLike = FunctionsStringSearch<TokenLikeImpl>;

}

REGISTER_FUNCTION(TokenLike)
{
    /// Function tokenLike can be only used in String column with token_bf index
    factory.registerFunction<FunctionTokenLike>();
}
}
