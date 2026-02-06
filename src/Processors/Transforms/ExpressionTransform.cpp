#include <memory>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>


namespace DB
{

Block ExpressionTransform::transformHeader(Block header, const ActionsDAG & dag, StorageMetadataPtr metadata_snapshot_)
{
    if (metadata_snapshot_ && metadata_snapshot_->hasImplicitColumn())
        MergeTreeDataWriter::fillMissingImplicitColumnsForSkipIndices(
            header,
            metadata_snapshot_,
            metadata_snapshot_->secondary_indices,
            std::make_shared<Names>(dag.getRequiredColumnsNames())
        );

    return dag.updateHeader(header);
}


ExpressionTransform::ExpressionTransform(const Block & header_, ExpressionActionsPtr expression_, StorageMetadataPtr metadata_snapshot_)
    : ISimpleTransform(header_, transformHeader(header_, expression_->getActionsDAG(), metadata_snapshot_), false)
    , expression(std::move(expression_))
    , metadata_snapshot(metadata_snapshot_)
{
}

void ExpressionTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    if (metadata_snapshot && metadata_snapshot->hasImplicitColumn())
        MergeTreeDataWriter::fillMissingImplicitColumnsForSkipIndices(
        block, metadata_snapshot,
        metadata_snapshot->secondary_indices,
        std::make_shared<Names>(expression->getActionsDAG().getRequiredColumnsNames())
        );

    expression->execute(block, num_rows);

    chunk.setColumns(block.getColumns(), num_rows);
}

ConvertingTransform::ConvertingTransform(const Block & header_, ExpressionActionsPtr expression_)
    : ExceptionKeepingTransform(header_, ExpressionTransform::transformHeader(header_, expression_->getActionsDAG()))
    , expression(std::move(expression_))
{
}

void ConvertingTransform::onConsume(Chunk chunk)
{
    size_t num_rows = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    expression->execute(block, num_rows);

    chunk.setColumns(block.getColumns(), num_rows);
    cur_chunk = std::move(chunk);
}

}
