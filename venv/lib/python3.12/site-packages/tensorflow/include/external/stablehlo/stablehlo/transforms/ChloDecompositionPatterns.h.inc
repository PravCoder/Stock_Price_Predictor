/*===- TableGen'erated file -------------------------------------*- C++ -*-===*\
|*                                                                            *|
|* Rewriters                                                                  *|
|*                                                                            *|
|* Automatically generated file, do not edit!                                 *|
|* From: ChloDecompositionPatterns.td                                         *|
|*                                                                            *|
\*===----------------------------------------------------------------------===*/


static ::mlir::LogicalResult __mlir_ods_local_type_constraint_ChloDecompositionPatterns0(
    ::mlir::PatternRewriter &rewriter, ::mlir::Operation *op, ::mlir::Type type,
    ::llvm::StringRef failureStr) {
  if (!((!isa<ComplexType>(cast<ShapedType>(type).getElementType())))) {
    return rewriter.notifyMatchFailure(op, [&](::mlir::Diagnostic &diag) {
      diag << failureStr << ": Non-complex element type";
    });
  }
  return ::mlir::success();
}

static ::mlir::LogicalResult __mlir_ods_local_type_constraint_ChloDecompositionPatterns1(
    ::mlir::PatternRewriter &rewriter, ::mlir::Operation *op, ::mlir::Type type,
    ::llvm::StringRef failureStr) {
  if (!((isa<ComplexType>(cast<ShapedType>(type).getElementType())))) {
    return rewriter.notifyMatchFailure(op, [&](::mlir::Diagnostic &diag) {
      diag << failureStr << ": Complex element type";
    });
  }
  return ::mlir::success();
}
/* Generated from:
    external/stablehlo/stablehlo/transforms/ChloDecompositionPatterns.td:53
*/
struct GeneratedConvert0 : public ::mlir::RewritePattern {
  GeneratedConvert0(::mlir::MLIRContext *context)
      : ::mlir::RewritePattern("chlo.acos", 1, context, {"stablehlo.add", "stablehlo.atan2", "stablehlo.compare", "stablehlo.multiply", "stablehlo.select", "stablehlo.sqrt", "stablehlo.subtract"}) {}
  ::mlir::LogicalResult matchAndRewrite(::mlir::Operation *op0,
      ::mlir::PatternRewriter &rewriter) const override {
    // Variables for capturing values and attributes used while creating ops
    ::mlir::Operation::operand_range input(op0->getOperands());
    ::llvm::SmallVector<::mlir::Operation *, 4> tblgen_ops;

    // Match
    tblgen_ops.push_back(op0);
    auto castedOp0 = ::llvm::dyn_cast<::mlir::chlo::AcosOp>(op0); (void)castedOp0;
    if(::mlir::failed(__mlir_ods_local_type_constraint_ChloDecompositionPatterns0(rewriter, castedOp0, (*castedOp0.getODSOperands(0).begin()).getType(), "operand 0 of op 'chlo.acos' failed to satisfy constraint: 'Non-complex element type'"))) {
      return ::mlir::failure();
    }
    input = castedOp0.getODSOperands(0);

    // Rewrite
    auto odsLoc = rewriter.getFusedLoc({tblgen_ops[0]->getLoc()}); (void)odsLoc;
    ::llvm::SmallVector<::mlir::Value, 4> tblgen_repl_values;
    auto nativeVar_0 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, -1, (*input.begin())); (void)nativeVar_0;
    auto nativeVar_1 = ::mlir::stablehlo::ComparisonTypeAttr(); (void)nativeVar_1;
    ::mlir::stablehlo::CompareOp tblgen_CompareOp_2;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      ::mlir::Value tblgen_value_1 = nativeVar_0;
      tblgen_CompareOp_2 = rewriter.create<::mlir::stablehlo::CompareOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1,
        ::mlir::stablehlo::ComparisonDirectionAttr::get(rewriter.getContext(), ::mlir::stablehlo::ComparisonDirection::NE),
        /*compare_type=*/nativeVar_1
      );
    }
    auto nativeVar_3 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, 2, (*input.begin())); (void)nativeVar_3;
    auto nativeVar_4 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, 1, (*input.begin())); (void)nativeVar_4;
    ::mlir::stablehlo::MulOp tblgen_MulOp_5;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      ::mlir::Value tblgen_value_1 = (*input.begin());
      tblgen_MulOp_5 = rewriter.create<::mlir::stablehlo::MulOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::SubtractOp tblgen_SubtractOp_6;
    {
      ::mlir::Value tblgen_value_0 = nativeVar_4;
      ::mlir::Value tblgen_value_1 = (*tblgen_MulOp_5.getODSResults(0).begin());
      tblgen_SubtractOp_6 = rewriter.create<::mlir::stablehlo::SubtractOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::SqrtOp tblgen_SqrtOp_7;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_SubtractOp_6.getODSResults(0).begin());
      tblgen_SqrtOp_7 = rewriter.create<::mlir::stablehlo::SqrtOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    auto nativeVar_8 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, 1, (*input.begin())); (void)nativeVar_8;
    ::mlir::stablehlo::AddOp tblgen_AddOp_9;
    {
      ::mlir::Value tblgen_value_0 = nativeVar_8;
      ::mlir::Value tblgen_value_1 = (*input.begin());
      tblgen_AddOp_9 = rewriter.create<::mlir::stablehlo::AddOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::Atan2Op tblgen_Atan2Op_10;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_SqrtOp_7.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = (*tblgen_AddOp_9.getODSResults(0).begin());
      tblgen_Atan2Op_10 = rewriter.create<::mlir::stablehlo::Atan2Op>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::MulOp tblgen_MulOp_11;
    {
      ::mlir::Value tblgen_value_0 = nativeVar_3;
      ::mlir::Value tblgen_value_1 = (*tblgen_Atan2Op_10.getODSResults(0).begin());
      tblgen_MulOp_11 = rewriter.create<::mlir::stablehlo::MulOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    auto nativeVar_12 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, M_PI, (*input.begin())); (void)nativeVar_12;
    ::mlir::stablehlo::SelectOp tblgen_SelectOp_13;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      tblgen_values.push_back((*tblgen_CompareOp_2.getODSResults(0).begin()));
      tblgen_values.push_back((*tblgen_MulOp_11.getODSResults(0).begin()));
      tblgen_values.push_back(nativeVar_12);
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(0)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_SelectOp_13 = rewriter.create<::mlir::stablehlo::SelectOp>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_SelectOp_13.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }

    rewriter.replaceOp(op0, tblgen_repl_values);
    return ::mlir::success();
  };
};

/* Generated from:
    external/stablehlo/stablehlo/transforms/ChloDecompositionPatterns.td:87
*/
struct GeneratedConvert1 : public ::mlir::RewritePattern {
  GeneratedConvert1(::mlir::MLIRContext *context)
      : ::mlir::RewritePattern("chlo.acosh", 1, context, {"stablehlo.add", "stablehlo.compare", "stablehlo.log", "stablehlo.multiply", "stablehlo.select", "stablehlo.sqrt"}) {}
  ::mlir::LogicalResult matchAndRewrite(::mlir::Operation *op0,
      ::mlir::PatternRewriter &rewriter) const override {
    // Variables for capturing values and attributes used while creating ops
    ::mlir::Operation::operand_range input(op0->getOperands());
    ::llvm::SmallVector<::mlir::Operation *, 4> tblgen_ops;

    // Match
    tblgen_ops.push_back(op0);
    auto castedOp0 = ::llvm::dyn_cast<::mlir::chlo::AcoshOp>(op0); (void)castedOp0;
    if(::mlir::failed(__mlir_ods_local_type_constraint_ChloDecompositionPatterns0(rewriter, castedOp0, (*castedOp0.getODSOperands(0).begin()).getType(), "operand 0 of op 'chlo.acosh' failed to satisfy constraint: 'Non-complex element type'"))) {
      return ::mlir::failure();
    }
    input = castedOp0.getODSOperands(0);

    // Rewrite
    auto odsLoc = rewriter.getFusedLoc({tblgen_ops[0]->getLoc()}); (void)odsLoc;
    ::llvm::SmallVector<::mlir::Value, 4> tblgen_repl_values;
    auto nativeVar_0 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, -1, (*input.begin())); (void)nativeVar_0;
    auto nativeVar_1 = ::mlir::stablehlo::ComparisonTypeAttr(); (void)nativeVar_1;
    ::mlir::stablehlo::CompareOp tblgen_CompareOp_2;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      ::mlir::Value tblgen_value_1 = nativeVar_0;
      tblgen_CompareOp_2 = rewriter.create<::mlir::stablehlo::CompareOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1,
        ::mlir::stablehlo::ComparisonDirectionAttr::get(rewriter.getContext(), ::mlir::stablehlo::ComparisonDirection::LT),
        /*compare_type=*/nativeVar_1
      );
    }
    auto nativeVar_3 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, NAN, (*input.begin())); (void)nativeVar_3;
    auto nativeVar_4 = ::mlir::stablehlo::getConstantLikeMaxFiniteValue(rewriter, odsLoc, (*input.begin())); (void)nativeVar_4;
    ::mlir::stablehlo::SqrtOp tblgen_SqrtOp_5;
    {
      ::mlir::Value tblgen_value_0 = nativeVar_4;
      tblgen_SqrtOp_5 = rewriter.create<::mlir::stablehlo::SqrtOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    auto nativeVar_6 = ::mlir::stablehlo::ComparisonTypeAttr(); (void)nativeVar_6;
    ::mlir::stablehlo::CompareOp tblgen_CompareOp_7;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      ::mlir::Value tblgen_value_1 = (*tblgen_SqrtOp_5.getODSResults(0).begin());
      tblgen_CompareOp_7 = rewriter.create<::mlir::stablehlo::CompareOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1,
        ::mlir::stablehlo::ComparisonDirectionAttr::get(rewriter.getContext(), ::mlir::stablehlo::ComparisonDirection::GE),
        /*compare_type=*/nativeVar_6
      );
    }
    ::mlir::stablehlo::LogOp tblgen_LogOp_8;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      tblgen_LogOp_8 = rewriter.create<::mlir::stablehlo::LogOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    auto nativeVar_9 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, 2, (*input.begin())); (void)nativeVar_9;
    ::mlir::stablehlo::LogOp tblgen_LogOp_10;
    {
      ::mlir::Value tblgen_value_0 = nativeVar_9;
      tblgen_LogOp_10 = rewriter.create<::mlir::stablehlo::LogOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::AddOp tblgen_AddOp_11;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_LogOp_8.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = (*tblgen_LogOp_10.getODSResults(0).begin());
      tblgen_AddOp_11 = rewriter.create<::mlir::stablehlo::AddOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    auto nativeVar_12 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, 1, (*input.begin())); (void)nativeVar_12;
    ::mlir::stablehlo::AddOp tblgen_AddOp_13;
    {
      ::mlir::Value tblgen_value_0 = nativeVar_12;
      ::mlir::Value tblgen_value_1 = (*input.begin());
      tblgen_AddOp_13 = rewriter.create<::mlir::stablehlo::AddOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    auto nativeVar_14 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, -1, (*input.begin())); (void)nativeVar_14;
    ::mlir::stablehlo::AddOp tblgen_AddOp_15;
    {
      ::mlir::Value tblgen_value_0 = nativeVar_14;
      ::mlir::Value tblgen_value_1 = (*input.begin());
      tblgen_AddOp_15 = rewriter.create<::mlir::stablehlo::AddOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::MulOp tblgen_MulOp_16;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_AddOp_13.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = (*tblgen_AddOp_15.getODSResults(0).begin());
      tblgen_MulOp_16 = rewriter.create<::mlir::stablehlo::MulOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::SqrtOp tblgen_SqrtOp_17;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_MulOp_16.getODSResults(0).begin());
      tblgen_SqrtOp_17 = rewriter.create<::mlir::stablehlo::SqrtOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::AddOp tblgen_AddOp_18;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      ::mlir::Value tblgen_value_1 = (*tblgen_SqrtOp_17.getODSResults(0).begin());
      tblgen_AddOp_18 = rewriter.create<::mlir::stablehlo::AddOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::LogOp tblgen_LogOp_19;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_AddOp_18.getODSResults(0).begin());
      tblgen_LogOp_19 = rewriter.create<::mlir::stablehlo::LogOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::SelectOp tblgen_SelectOp_20;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_CompareOp_7.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = (*tblgen_AddOp_11.getODSResults(0).begin());
      ::mlir::Value tblgen_value_2 = (*tblgen_LogOp_19.getODSResults(0).begin());
      tblgen_SelectOp_20 = rewriter.create<::mlir::stablehlo::SelectOp>(odsLoc,
        /*pred=*/tblgen_value_0,
        /*on_true=*/tblgen_value_1,
        /*on_false=*/tblgen_value_2
      );
    }
    ::mlir::stablehlo::SelectOp tblgen_SelectOp_21;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      tblgen_values.push_back((*tblgen_CompareOp_2.getODSResults(0).begin()));
      tblgen_values.push_back(nativeVar_3);
      tblgen_values.push_back((*tblgen_SelectOp_20.getODSResults(0).begin()));
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(0)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_SelectOp_21 = rewriter.create<::mlir::stablehlo::SelectOp>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_SelectOp_21.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }

    rewriter.replaceOp(op0, tblgen_repl_values);
    return ::mlir::success();
  };
};

/* Generated from:
    external/stablehlo/stablehlo/transforms/ChloDecompositionPatterns.td:138
*/
struct GeneratedConvert2 : public ::mlir::RewritePattern {
  GeneratedConvert2(::mlir::MLIRContext *context)
      : ::mlir::RewritePattern("chlo.acosh", 1, context, {"stablehlo.add", "stablehlo.log", "stablehlo.multiply", "stablehlo.sqrt", "stablehlo.subtract"}) {}
  ::mlir::LogicalResult matchAndRewrite(::mlir::Operation *op0,
      ::mlir::PatternRewriter &rewriter) const override {
    // Variables for capturing values and attributes used while creating ops
    ::mlir::Operation::operand_range input(op0->getOperands());
    ::llvm::SmallVector<::mlir::Operation *, 4> tblgen_ops;

    // Match
    tblgen_ops.push_back(op0);
    auto castedOp0 = ::llvm::dyn_cast<::mlir::chlo::AcoshOp>(op0); (void)castedOp0;
    if(::mlir::failed(__mlir_ods_local_type_constraint_ChloDecompositionPatterns1(rewriter, castedOp0, (*castedOp0.getODSOperands(0).begin()).getType(), "operand 0 of op 'chlo.acosh' failed to satisfy constraint: 'Complex element type'"))) {
      return ::mlir::failure();
    }
    input = castedOp0.getODSOperands(0);

    // Rewrite
    auto odsLoc = rewriter.getFusedLoc({tblgen_ops[0]->getLoc()}); (void)odsLoc;
    ::llvm::SmallVector<::mlir::Value, 4> tblgen_repl_values;
    auto nativeVar_0 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, 1, (*input.begin())); (void)nativeVar_0;
    ::mlir::stablehlo::AddOp tblgen_AddOp_1;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      ::mlir::Value tblgen_value_1 = nativeVar_0;
      tblgen_AddOp_1 = rewriter.create<::mlir::stablehlo::AddOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    auto nativeVar_2 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, 1, (*input.begin())); (void)nativeVar_2;
    ::mlir::stablehlo::SubtractOp tblgen_SubtractOp_3;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      ::mlir::Value tblgen_value_1 = nativeVar_2;
      tblgen_SubtractOp_3 = rewriter.create<::mlir::stablehlo::SubtractOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::MulOp tblgen_MulOp_4;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_AddOp_1.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = (*tblgen_SubtractOp_3.getODSResults(0).begin());
      tblgen_MulOp_4 = rewriter.create<::mlir::stablehlo::MulOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::SqrtOp tblgen_SqrtOp_5;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_MulOp_4.getODSResults(0).begin());
      tblgen_SqrtOp_5 = rewriter.create<::mlir::stablehlo::SqrtOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::AddOp tblgen_AddOp_6;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      ::mlir::Value tblgen_value_1 = (*tblgen_SqrtOp_5.getODSResults(0).begin());
      tblgen_AddOp_6 = rewriter.create<::mlir::stablehlo::AddOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::LogOp tblgen_LogOp_7;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      tblgen_values.push_back((*tblgen_AddOp_6.getODSResults(0).begin()));
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(0)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_LogOp_7 = rewriter.create<::mlir::stablehlo::LogOp>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_LogOp_7.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }

    rewriter.replaceOp(op0, tblgen_repl_values);
    return ::mlir::success();
  };
};

/* Generated from:
    external/stablehlo/stablehlo/transforms/ChloDecompositionPatterns.td:160
*/
struct GeneratedConvert3 : public ::mlir::RewritePattern {
  GeneratedConvert3(::mlir::MLIRContext *context)
      : ::mlir::RewritePattern("chlo.asin", 1, context, {"stablehlo.add", "stablehlo.atan2", "stablehlo.multiply", "stablehlo.sqrt", "stablehlo.subtract"}) {}
  ::mlir::LogicalResult matchAndRewrite(::mlir::Operation *op0,
      ::mlir::PatternRewriter &rewriter) const override {
    // Variables for capturing values and attributes used while creating ops
    ::mlir::Operation::operand_range input(op0->getOperands());
    ::llvm::SmallVector<::mlir::Operation *, 4> tblgen_ops;

    // Match
    tblgen_ops.push_back(op0);
    auto castedOp0 = ::llvm::dyn_cast<::mlir::chlo::AsinOp>(op0); (void)castedOp0;
    input = castedOp0.getODSOperands(0);

    // Rewrite
    auto odsLoc = rewriter.getFusedLoc({tblgen_ops[0]->getLoc()}); (void)odsLoc;
    ::llvm::SmallVector<::mlir::Value, 4> tblgen_repl_values;
    auto nativeVar_0 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, 2, (*input.begin())); (void)nativeVar_0;
    auto nativeVar_1 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, 1, (*input.begin())); (void)nativeVar_1;
    auto nativeVar_2 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, 1, (*input.begin())); (void)nativeVar_2;
    ::mlir::stablehlo::MulOp tblgen_MulOp_3;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      ::mlir::Value tblgen_value_1 = (*input.begin());
      tblgen_MulOp_3 = rewriter.create<::mlir::stablehlo::MulOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::SubtractOp tblgen_SubtractOp_4;
    {
      ::mlir::Value tblgen_value_0 = nativeVar_2;
      ::mlir::Value tblgen_value_1 = (*tblgen_MulOp_3.getODSResults(0).begin());
      tblgen_SubtractOp_4 = rewriter.create<::mlir::stablehlo::SubtractOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::SqrtOp tblgen_SqrtOp_5;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_SubtractOp_4.getODSResults(0).begin());
      tblgen_SqrtOp_5 = rewriter.create<::mlir::stablehlo::SqrtOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::AddOp tblgen_AddOp_6;
    {
      ::mlir::Value tblgen_value_0 = nativeVar_1;
      ::mlir::Value tblgen_value_1 = (*tblgen_SqrtOp_5.getODSResults(0).begin());
      tblgen_AddOp_6 = rewriter.create<::mlir::stablehlo::AddOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::Atan2Op tblgen_Atan2Op_7;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      ::mlir::Value tblgen_value_1 = (*tblgen_AddOp_6.getODSResults(0).begin());
      tblgen_Atan2Op_7 = rewriter.create<::mlir::stablehlo::Atan2Op>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::MulOp tblgen_MulOp_8;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      tblgen_values.push_back(nativeVar_0);
      tblgen_values.push_back((*tblgen_Atan2Op_7.getODSResults(0).begin()));
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(0)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_MulOp_8 = rewriter.create<::mlir::stablehlo::MulOp>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_MulOp_8.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }

    rewriter.replaceOp(op0, tblgen_repl_values);
    return ::mlir::success();
  };
};

/* Generated from:
    external/stablehlo/stablehlo/transforms/ChloDecompositionPatterns.td:197
*/
struct GeneratedConvert4 : public ::mlir::RewritePattern {
  GeneratedConvert4(::mlir::MLIRContext *context)
      : ::mlir::RewritePattern("chlo.asinh", 1, context, {"stablehlo.abs", "stablehlo.add", "stablehlo.compare", "stablehlo.divide", "stablehlo.log", "stablehlo.log_plus_one", "stablehlo.multiply", "stablehlo.select", "stablehlo.sign", "stablehlo.sqrt"}) {}
  ::mlir::LogicalResult matchAndRewrite(::mlir::Operation *op0,
      ::mlir::PatternRewriter &rewriter) const override {
    // Variables for capturing values and attributes used while creating ops
    ::mlir::Operation::operand_range input(op0->getOperands());
    ::llvm::SmallVector<::mlir::Operation *, 4> tblgen_ops;

    // Match
    tblgen_ops.push_back(op0);
    auto castedOp0 = ::llvm::dyn_cast<::mlir::chlo::AsinhOp>(op0); (void)castedOp0;
    if(::mlir::failed(__mlir_ods_local_type_constraint_ChloDecompositionPatterns0(rewriter, castedOp0, (*castedOp0.getODSOperands(0).begin()).getType(), "operand 0 of op 'chlo.asinh' failed to satisfy constraint: 'Non-complex element type'"))) {
      return ::mlir::failure();
    }
    input = castedOp0.getODSOperands(0);

    // Rewrite
    auto odsLoc = rewriter.getFusedLoc({tblgen_ops[0]->getLoc()}); (void)odsLoc;
    ::llvm::SmallVector<::mlir::Value, 4> tblgen_repl_values;
    ::mlir::stablehlo::SignOp tblgen_SignOp_0;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      tblgen_SignOp_0 = rewriter.create<::mlir::stablehlo::SignOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::AbsOp tblgen_AbsOp_1;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      tblgen_AbsOp_1 = rewriter.create<::mlir::stablehlo::AbsOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    auto nativeVar_2 = ::mlir::stablehlo::getConstantLikeMaxFiniteValue(rewriter, odsLoc, (*input.begin())); (void)nativeVar_2;
    ::mlir::stablehlo::SqrtOp tblgen_SqrtOp_3;
    {
      ::mlir::Value tblgen_value_0 = nativeVar_2;
      tblgen_SqrtOp_3 = rewriter.create<::mlir::stablehlo::SqrtOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    auto nativeVar_4 = ::mlir::stablehlo::ComparisonTypeAttr(); (void)nativeVar_4;
    ::mlir::stablehlo::CompareOp tblgen_CompareOp_5;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_AbsOp_1.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = (*tblgen_SqrtOp_3.getODSResults(0).begin());
      tblgen_CompareOp_5 = rewriter.create<::mlir::stablehlo::CompareOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1,
        ::mlir::stablehlo::ComparisonDirectionAttr::get(rewriter.getContext(), ::mlir::stablehlo::ComparisonDirection::GE),
        /*compare_type=*/nativeVar_4
      );
    }
    ::mlir::stablehlo::AbsOp tblgen_AbsOp_6;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      tblgen_AbsOp_6 = rewriter.create<::mlir::stablehlo::AbsOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::LogOp tblgen_LogOp_7;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_AbsOp_6.getODSResults(0).begin());
      tblgen_LogOp_7 = rewriter.create<::mlir::stablehlo::LogOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    auto nativeVar_8 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, 2, (*input.begin())); (void)nativeVar_8;
    ::mlir::stablehlo::LogOp tblgen_LogOp_9;
    {
      ::mlir::Value tblgen_value_0 = nativeVar_8;
      tblgen_LogOp_9 = rewriter.create<::mlir::stablehlo::LogOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::AddOp tblgen_AddOp_10;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_LogOp_7.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = (*tblgen_LogOp_9.getODSResults(0).begin());
      tblgen_AddOp_10 = rewriter.create<::mlir::stablehlo::AddOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::AbsOp tblgen_AbsOp_11;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      tblgen_AbsOp_11 = rewriter.create<::mlir::stablehlo::AbsOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    auto nativeVar_12 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, 1, (*input.begin())); (void)nativeVar_12;
    auto nativeVar_13 = ::mlir::stablehlo::ComparisonTypeAttr(); (void)nativeVar_13;
    ::mlir::stablehlo::CompareOp tblgen_CompareOp_14;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_AbsOp_11.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = nativeVar_12;
      tblgen_CompareOp_14 = rewriter.create<::mlir::stablehlo::CompareOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1,
        ::mlir::stablehlo::ComparisonDirectionAttr::get(rewriter.getContext(), ::mlir::stablehlo::ComparisonDirection::LE),
        /*compare_type=*/nativeVar_13
      );
    }
    ::mlir::stablehlo::AbsOp tblgen_AbsOp_15;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      tblgen_AbsOp_15 = rewriter.create<::mlir::stablehlo::AbsOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::AbsOp tblgen_AbsOp_16;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      tblgen_AbsOp_16 = rewriter.create<::mlir::stablehlo::AbsOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::AbsOp tblgen_AbsOp_17;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      tblgen_AbsOp_17 = rewriter.create<::mlir::stablehlo::AbsOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    auto nativeVar_18 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, 1, (*input.begin())); (void)nativeVar_18;
    ::mlir::stablehlo::AbsOp tblgen_AbsOp_19;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      tblgen_AbsOp_19 = rewriter.create<::mlir::stablehlo::AbsOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::AbsOp tblgen_AbsOp_20;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      tblgen_AbsOp_20 = rewriter.create<::mlir::stablehlo::AbsOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::MulOp tblgen_MulOp_21;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_AbsOp_19.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = (*tblgen_AbsOp_20.getODSResults(0).begin());
      tblgen_MulOp_21 = rewriter.create<::mlir::stablehlo::MulOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    auto nativeVar_22 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, 1, (*input.begin())); (void)nativeVar_22;
    ::mlir::stablehlo::AddOp tblgen_AddOp_23;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_MulOp_21.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = nativeVar_22;
      tblgen_AddOp_23 = rewriter.create<::mlir::stablehlo::AddOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::SqrtOp tblgen_SqrtOp_24;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_AddOp_23.getODSResults(0).begin());
      tblgen_SqrtOp_24 = rewriter.create<::mlir::stablehlo::SqrtOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::AddOp tblgen_AddOp_25;
    {
      ::mlir::Value tblgen_value_0 = nativeVar_18;
      ::mlir::Value tblgen_value_1 = (*tblgen_SqrtOp_24.getODSResults(0).begin());
      tblgen_AddOp_25 = rewriter.create<::mlir::stablehlo::AddOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::DivOp tblgen_DivOp_26;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_AbsOp_17.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = (*tblgen_AddOp_25.getODSResults(0).begin());
      tblgen_DivOp_26 = rewriter.create<::mlir::stablehlo::DivOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::MulOp tblgen_MulOp_27;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_AbsOp_16.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = (*tblgen_DivOp_26.getODSResults(0).begin());
      tblgen_MulOp_27 = rewriter.create<::mlir::stablehlo::MulOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::AddOp tblgen_AddOp_28;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_AbsOp_15.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = (*tblgen_MulOp_27.getODSResults(0).begin());
      tblgen_AddOp_28 = rewriter.create<::mlir::stablehlo::AddOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::Log1pOp tblgen_Log1pOp_29;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_AddOp_28.getODSResults(0).begin());
      tblgen_Log1pOp_29 = rewriter.create<::mlir::stablehlo::Log1pOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::AbsOp tblgen_AbsOp_30;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      tblgen_AbsOp_30 = rewriter.create<::mlir::stablehlo::AbsOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::AbsOp tblgen_AbsOp_31;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      tblgen_AbsOp_31 = rewriter.create<::mlir::stablehlo::AbsOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::AbsOp tblgen_AbsOp_32;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      tblgen_AbsOp_32 = rewriter.create<::mlir::stablehlo::AbsOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::MulOp tblgen_MulOp_33;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_AbsOp_31.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = (*tblgen_AbsOp_32.getODSResults(0).begin());
      tblgen_MulOp_33 = rewriter.create<::mlir::stablehlo::MulOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    auto nativeVar_34 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, 1, (*input.begin())); (void)nativeVar_34;
    ::mlir::stablehlo::AddOp tblgen_AddOp_35;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_MulOp_33.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = nativeVar_34;
      tblgen_AddOp_35 = rewriter.create<::mlir::stablehlo::AddOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::SqrtOp tblgen_SqrtOp_36;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_AddOp_35.getODSResults(0).begin());
      tblgen_SqrtOp_36 = rewriter.create<::mlir::stablehlo::SqrtOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::AddOp tblgen_AddOp_37;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_AbsOp_30.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = (*tblgen_SqrtOp_36.getODSResults(0).begin());
      tblgen_AddOp_37 = rewriter.create<::mlir::stablehlo::AddOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::LogOp tblgen_LogOp_38;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_AddOp_37.getODSResults(0).begin());
      tblgen_LogOp_38 = rewriter.create<::mlir::stablehlo::LogOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::SelectOp tblgen_SelectOp_39;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_CompareOp_14.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = (*tblgen_Log1pOp_29.getODSResults(0).begin());
      ::mlir::Value tblgen_value_2 = (*tblgen_LogOp_38.getODSResults(0).begin());
      tblgen_SelectOp_39 = rewriter.create<::mlir::stablehlo::SelectOp>(odsLoc,
        /*pred=*/tblgen_value_0,
        /*on_true=*/tblgen_value_1,
        /*on_false=*/tblgen_value_2
      );
    }
    ::mlir::stablehlo::SelectOp tblgen_SelectOp_40;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_CompareOp_5.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = (*tblgen_AddOp_10.getODSResults(0).begin());
      ::mlir::Value tblgen_value_2 = (*tblgen_SelectOp_39.getODSResults(0).begin());
      tblgen_SelectOp_40 = rewriter.create<::mlir::stablehlo::SelectOp>(odsLoc,
        /*pred=*/tblgen_value_0,
        /*on_true=*/tblgen_value_1,
        /*on_false=*/tblgen_value_2
      );
    }
    ::mlir::stablehlo::MulOp tblgen_MulOp_41;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      tblgen_values.push_back((*tblgen_SignOp_0.getODSResults(0).begin()));
      tblgen_values.push_back((*tblgen_SelectOp_40.getODSResults(0).begin()));
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(0)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_MulOp_41 = rewriter.create<::mlir::stablehlo::MulOp>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_MulOp_41.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }

    rewriter.replaceOp(op0, tblgen_repl_values);
    return ::mlir::success();
  };
};

/* Generated from:
    external/stablehlo/stablehlo/transforms/ChloDecompositionPatterns.td:272
*/
struct GeneratedConvert5 : public ::mlir::RewritePattern {
  GeneratedConvert5(::mlir::MLIRContext *context)
      : ::mlir::RewritePattern("chlo.asinh", 1, context, {"stablehlo.add", "stablehlo.log", "stablehlo.multiply", "stablehlo.sqrt"}) {}
  ::mlir::LogicalResult matchAndRewrite(::mlir::Operation *op0,
      ::mlir::PatternRewriter &rewriter) const override {
    // Variables for capturing values and attributes used while creating ops
    ::mlir::Operation::operand_range input(op0->getOperands());
    ::llvm::SmallVector<::mlir::Operation *, 4> tblgen_ops;

    // Match
    tblgen_ops.push_back(op0);
    auto castedOp0 = ::llvm::dyn_cast<::mlir::chlo::AsinhOp>(op0); (void)castedOp0;
    if(::mlir::failed(__mlir_ods_local_type_constraint_ChloDecompositionPatterns1(rewriter, castedOp0, (*castedOp0.getODSOperands(0).begin()).getType(), "operand 0 of op 'chlo.asinh' failed to satisfy constraint: 'Complex element type'"))) {
      return ::mlir::failure();
    }
    input = castedOp0.getODSOperands(0);

    // Rewrite
    auto odsLoc = rewriter.getFusedLoc({tblgen_ops[0]->getLoc()}); (void)odsLoc;
    ::llvm::SmallVector<::mlir::Value, 4> tblgen_repl_values;
    ::mlir::stablehlo::MulOp tblgen_MulOp_0;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      ::mlir::Value tblgen_value_1 = (*input.begin());
      tblgen_MulOp_0 = rewriter.create<::mlir::stablehlo::MulOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    auto nativeVar_1 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, 1, (*input.begin())); (void)nativeVar_1;
    ::mlir::stablehlo::AddOp tblgen_AddOp_2;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_MulOp_0.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = nativeVar_1;
      tblgen_AddOp_2 = rewriter.create<::mlir::stablehlo::AddOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::SqrtOp tblgen_SqrtOp_3;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_AddOp_2.getODSResults(0).begin());
      tblgen_SqrtOp_3 = rewriter.create<::mlir::stablehlo::SqrtOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::AddOp tblgen_AddOp_4;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      ::mlir::Value tblgen_value_1 = (*tblgen_SqrtOp_3.getODSResults(0).begin());
      tblgen_AddOp_4 = rewriter.create<::mlir::stablehlo::AddOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::LogOp tblgen_LogOp_5;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      tblgen_values.push_back((*tblgen_AddOp_4.getODSResults(0).begin()));
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(0)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_LogOp_5 = rewriter.create<::mlir::stablehlo::LogOp>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_LogOp_5.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }

    rewriter.replaceOp(op0, tblgen_repl_values);
    return ::mlir::success();
  };
};

/* Generated from:
    external/stablehlo/stablehlo/transforms/ChloDecompositionPatterns.td:287
*/
struct GeneratedConvert6 : public ::mlir::RewritePattern {
  GeneratedConvert6(::mlir::MLIRContext *context)
      : ::mlir::RewritePattern("chlo.atan", 1, context, {"stablehlo.atan2"}) {}
  ::mlir::LogicalResult matchAndRewrite(::mlir::Operation *op0,
      ::mlir::PatternRewriter &rewriter) const override {
    // Variables for capturing values and attributes used while creating ops
    ::mlir::Operation::operand_range input(op0->getOperands());
    ::llvm::SmallVector<::mlir::Operation *, 4> tblgen_ops;

    // Match
    tblgen_ops.push_back(op0);
    auto castedOp0 = ::llvm::dyn_cast<::mlir::chlo::AtanOp>(op0); (void)castedOp0;
    input = castedOp0.getODSOperands(0);

    // Rewrite
    auto odsLoc = rewriter.getFusedLoc({tblgen_ops[0]->getLoc()}); (void)odsLoc;
    ::llvm::SmallVector<::mlir::Value, 4> tblgen_repl_values;
    auto nativeVar_0 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, 1, (*input.begin())); (void)nativeVar_0;
    ::mlir::stablehlo::Atan2Op tblgen_Atan2Op_1;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      tblgen_values.push_back((*input.begin()));
      tblgen_values.push_back(nativeVar_0);
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(0)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_Atan2Op_1 = rewriter.create<::mlir::stablehlo::Atan2Op>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_Atan2Op_1.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }

    rewriter.replaceOp(op0, tblgen_repl_values);
    return ::mlir::success();
  };
};

/* Generated from:
    external/stablehlo/stablehlo/transforms/ChloDecompositionPatterns.td:296
*/
struct GeneratedConvert7 : public ::mlir::RewritePattern {
  GeneratedConvert7(::mlir::MLIRContext *context)
      : ::mlir::RewritePattern("chlo.atanh", 1, context, {"stablehlo.abs", "stablehlo.compare", "stablehlo.log_plus_one", "stablehlo.multiply", "stablehlo.negate", "stablehlo.select", "stablehlo.subtract"}) {}
  ::mlir::LogicalResult matchAndRewrite(::mlir::Operation *op0,
      ::mlir::PatternRewriter &rewriter) const override {
    // Variables for capturing values and attributes used while creating ops
    ::mlir::Operation::operand_range input(op0->getOperands());
    ::llvm::SmallVector<::mlir::Operation *, 4> tblgen_ops;

    // Match
    tblgen_ops.push_back(op0);
    auto castedOp0 = ::llvm::dyn_cast<::mlir::chlo::AtanhOp>(op0); (void)castedOp0;
    if(::mlir::failed(__mlir_ods_local_type_constraint_ChloDecompositionPatterns0(rewriter, castedOp0, (*castedOp0.getODSOperands(0).begin()).getType(), "operand 0 of op 'chlo.atanh' failed to satisfy constraint: 'Non-complex element type'"))) {
      return ::mlir::failure();
    }
    input = castedOp0.getODSOperands(0);

    // Rewrite
    auto odsLoc = rewriter.getFusedLoc({tblgen_ops[0]->getLoc()}); (void)odsLoc;
    ::llvm::SmallVector<::mlir::Value, 4> tblgen_repl_values;
    ::mlir::stablehlo::AbsOp tblgen_AbsOp_0;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      tblgen_AbsOp_0 = rewriter.create<::mlir::stablehlo::AbsOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    auto nativeVar_1 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, 1, (*input.begin())); (void)nativeVar_1;
    auto nativeVar_2 = ::mlir::stablehlo::ComparisonTypeAttr(); (void)nativeVar_2;
    ::mlir::stablehlo::CompareOp tblgen_CompareOp_3;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_AbsOp_0.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = nativeVar_1;
      tblgen_CompareOp_3 = rewriter.create<::mlir::stablehlo::CompareOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1,
        ::mlir::stablehlo::ComparisonDirectionAttr::get(rewriter.getContext(), ::mlir::stablehlo::ComparisonDirection::GT),
        /*compare_type=*/nativeVar_2
      );
    }
    auto nativeVar_4 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, NAN, (*input.begin())); (void)nativeVar_4;
    ::mlir::stablehlo::Log1pOp tblgen_Log1pOp_5;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      tblgen_Log1pOp_5 = rewriter.create<::mlir::stablehlo::Log1pOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::NegOp tblgen_NegOp_6;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      tblgen_NegOp_6 = rewriter.create<::mlir::stablehlo::NegOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::Log1pOp tblgen_Log1pOp_7;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_NegOp_6.getODSResults(0).begin());
      tblgen_Log1pOp_7 = rewriter.create<::mlir::stablehlo::Log1pOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::SubtractOp tblgen_SubtractOp_8;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_Log1pOp_5.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = (*tblgen_Log1pOp_7.getODSResults(0).begin());
      tblgen_SubtractOp_8 = rewriter.create<::mlir::stablehlo::SubtractOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    auto nativeVar_9 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, 0.5, (*input.begin())); (void)nativeVar_9;
    ::mlir::stablehlo::MulOp tblgen_MulOp_10;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_SubtractOp_8.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = nativeVar_9;
      tblgen_MulOp_10 = rewriter.create<::mlir::stablehlo::MulOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::SelectOp tblgen_SelectOp_11;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      tblgen_values.push_back((*tblgen_CompareOp_3.getODSResults(0).begin()));
      tblgen_values.push_back(nativeVar_4);
      tblgen_values.push_back((*tblgen_MulOp_10.getODSResults(0).begin()));
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(0)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_SelectOp_11 = rewriter.create<::mlir::stablehlo::SelectOp>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_SelectOp_11.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }

    rewriter.replaceOp(op0, tblgen_repl_values);
    return ::mlir::success();
  };
};

/* Generated from:
    external/stablehlo/stablehlo/transforms/ChloDecompositionPatterns.td:322
*/
struct GeneratedConvert8 : public ::mlir::RewritePattern {
  GeneratedConvert8(::mlir::MLIRContext *context)
      : ::mlir::RewritePattern("chlo.atanh", 1, context, {"stablehlo.log_plus_one", "stablehlo.multiply", "stablehlo.negate", "stablehlo.subtract"}) {}
  ::mlir::LogicalResult matchAndRewrite(::mlir::Operation *op0,
      ::mlir::PatternRewriter &rewriter) const override {
    // Variables for capturing values and attributes used while creating ops
    ::mlir::Operation::operand_range input(op0->getOperands());
    ::llvm::SmallVector<::mlir::Operation *, 4> tblgen_ops;

    // Match
    tblgen_ops.push_back(op0);
    auto castedOp0 = ::llvm::dyn_cast<::mlir::chlo::AtanhOp>(op0); (void)castedOp0;
    if(::mlir::failed(__mlir_ods_local_type_constraint_ChloDecompositionPatterns1(rewriter, castedOp0, (*castedOp0.getODSOperands(0).begin()).getType(), "operand 0 of op 'chlo.atanh' failed to satisfy constraint: 'Complex element type'"))) {
      return ::mlir::failure();
    }
    input = castedOp0.getODSOperands(0);

    // Rewrite
    auto odsLoc = rewriter.getFusedLoc({tblgen_ops[0]->getLoc()}); (void)odsLoc;
    ::llvm::SmallVector<::mlir::Value, 4> tblgen_repl_values;
    ::mlir::stablehlo::Log1pOp tblgen_Log1pOp_0;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      tblgen_Log1pOp_0 = rewriter.create<::mlir::stablehlo::Log1pOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::NegOp tblgen_NegOp_1;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      tblgen_NegOp_1 = rewriter.create<::mlir::stablehlo::NegOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::Log1pOp tblgen_Log1pOp_2;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_NegOp_1.getODSResults(0).begin());
      tblgen_Log1pOp_2 = rewriter.create<::mlir::stablehlo::Log1pOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::SubtractOp tblgen_SubtractOp_3;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_Log1pOp_0.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = (*tblgen_Log1pOp_2.getODSResults(0).begin());
      tblgen_SubtractOp_3 = rewriter.create<::mlir::stablehlo::SubtractOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    auto nativeVar_4 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, 0.5, (*input.begin())); (void)nativeVar_4;
    ::mlir::stablehlo::MulOp tblgen_MulOp_5;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      tblgen_values.push_back((*tblgen_SubtractOp_3.getODSResults(0).begin()));
      tblgen_values.push_back(nativeVar_4);
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(0)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_MulOp_5 = rewriter.create<::mlir::stablehlo::MulOp>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_MulOp_5.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }

    rewriter.replaceOp(op0, tblgen_repl_values);
    return ::mlir::success();
  };
};

/* Generated from:
    external/stablehlo/stablehlo/transforms/ChloDecompositionPatterns.td:335
*/
struct GeneratedConvert9 : public ::mlir::RewritePattern {
  GeneratedConvert9(::mlir::MLIRContext *context)
      : ::mlir::RewritePattern("chlo.conj", 1, context, {"stablehlo.complex", "stablehlo.imag", "stablehlo.negate", "stablehlo.real"}) {}
  ::mlir::LogicalResult matchAndRewrite(::mlir::Operation *op0,
      ::mlir::PatternRewriter &rewriter) const override {
    // Variables for capturing values and attributes used while creating ops
    ::mlir::Operation::operand_range v(op0->getOperands());
    ::llvm::SmallVector<::mlir::Operation *, 4> tblgen_ops;

    // Match
    tblgen_ops.push_back(op0);
    auto castedOp0 = ::llvm::dyn_cast<::mlir::chlo::ConjOp>(op0); (void)castedOp0;
    v = castedOp0.getODSOperands(0);

    // Rewrite
    auto odsLoc = rewriter.getFusedLoc({tblgen_ops[0]->getLoc()}); (void)odsLoc;
    ::llvm::SmallVector<::mlir::Value, 4> tblgen_repl_values;
    ::mlir::stablehlo::RealOp tblgen_RealOp_0;
    {
      ::mlir::Value tblgen_value_0 = (*v.begin());
      tblgen_RealOp_0 = rewriter.create<::mlir::stablehlo::RealOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::ImagOp tblgen_ImagOp_1;
    {
      ::mlir::Value tblgen_value_0 = (*v.begin());
      tblgen_ImagOp_1 = rewriter.create<::mlir::stablehlo::ImagOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::NegOp tblgen_NegOp_2;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_ImagOp_1.getODSResults(0).begin());
      tblgen_NegOp_2 = rewriter.create<::mlir::stablehlo::NegOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::ComplexOp tblgen_ComplexOp_3;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      tblgen_values.push_back((*tblgen_RealOp_0.getODSResults(0).begin()));
      tblgen_values.push_back((*tblgen_NegOp_2.getODSResults(0).begin()));
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(0)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_ComplexOp_3 = rewriter.create<::mlir::stablehlo::ComplexOp>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_ComplexOp_3.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }

    rewriter.replaceOp(op0, tblgen_repl_values);
    return ::mlir::success();
  };
};

/* Generated from:
    external/stablehlo/stablehlo/transforms/ChloDecompositionPatterns.td:340
*/
struct GeneratedConvert10 : public ::mlir::RewritePattern {
  GeneratedConvert10(::mlir::MLIRContext *context)
      : ::mlir::RewritePattern("chlo.is_inf", 1, context, {"chlo.is_pos_inf", "stablehlo.abs"}) {}
  ::mlir::LogicalResult matchAndRewrite(::mlir::Operation *op0,
      ::mlir::PatternRewriter &rewriter) const override {
    // Variables for capturing values and attributes used while creating ops
    ::mlir::Operation::operand_range input(op0->getOperands());
    ::llvm::SmallVector<::mlir::Operation *, 4> tblgen_ops;

    // Match
    tblgen_ops.push_back(op0);
    auto castedOp0 = ::llvm::dyn_cast<::mlir::chlo::IsInfOp>(op0); (void)castedOp0;
    if(::mlir::failed(__mlir_ods_local_type_constraint_ChloDecompositionPatterns0(rewriter, castedOp0, (*castedOp0.getODSOperands(0).begin()).getType(), "operand 0 of op 'chlo.is_inf' failed to satisfy constraint: 'Non-complex element type'"))) {
      return ::mlir::failure();
    }
    input = castedOp0.getODSOperands(0);

    // Rewrite
    auto odsLoc = rewriter.getFusedLoc({tblgen_ops[0]->getLoc()}); (void)odsLoc;
    ::llvm::SmallVector<::mlir::Value, 4> tblgen_repl_values;
    ::mlir::stablehlo::AbsOp tblgen_AbsOp_0;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      tblgen_AbsOp_0 = rewriter.create<::mlir::stablehlo::AbsOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::chlo::IsPosInfOp tblgen_IsPosInfOp_1;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      tblgen_values.push_back((*tblgen_AbsOp_0.getODSResults(0).begin()));
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(0)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_IsPosInfOp_1 = rewriter.create<::mlir::chlo::IsPosInfOp>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_IsPosInfOp_1.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }

    rewriter.replaceOp(op0, tblgen_repl_values);
    return ::mlir::success();
  };
};

/* Generated from:
    external/stablehlo/stablehlo/transforms/ChloDecompositionPatterns.td:347
*/
struct GeneratedConvert11 : public ::mlir::RewritePattern {
  GeneratedConvert11(::mlir::MLIRContext *context)
      : ::mlir::RewritePattern("chlo.is_pos_inf", 1, context, {"stablehlo.compare"}) {}
  ::mlir::LogicalResult matchAndRewrite(::mlir::Operation *op0,
      ::mlir::PatternRewriter &rewriter) const override {
    // Variables for capturing values and attributes used while creating ops
    ::mlir::Operation::operand_range input(op0->getOperands());
    ::llvm::SmallVector<::mlir::Operation *, 4> tblgen_ops;

    // Match
    tblgen_ops.push_back(op0);
    auto castedOp0 = ::llvm::dyn_cast<::mlir::chlo::IsPosInfOp>(op0); (void)castedOp0;
    if(::mlir::failed(__mlir_ods_local_type_constraint_ChloDecompositionPatterns0(rewriter, castedOp0, (*castedOp0.getODSOperands(0).begin()).getType(), "operand 0 of op 'chlo.is_pos_inf' failed to satisfy constraint: 'Non-complex element type'"))) {
      return ::mlir::failure();
    }
    input = castedOp0.getODSOperands(0);

    // Rewrite
    auto odsLoc = rewriter.getFusedLoc({tblgen_ops[0]->getLoc()}); (void)odsLoc;
    ::llvm::SmallVector<::mlir::Value, 4> tblgen_repl_values;
    auto nativeVar_0 = ::mlir::stablehlo::getConstantLikeInfValue(rewriter, odsLoc, (*input.begin()), /*negative=*/false); (void)nativeVar_0;
    auto nativeVar_1 = ::mlir::stablehlo::ComparisonTypeAttr(); (void)nativeVar_1;
    ::mlir::stablehlo::CompareOp tblgen_CompareOp_2;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      tblgen_values.push_back((*input.begin()));
      tblgen_values.push_back(nativeVar_0);
      if (auto tmpAttr = ::mlir::stablehlo::ComparisonDirectionAttr::get(rewriter.getContext(), ::mlir::stablehlo::ComparisonDirection::EQ)) {
        tblgen_attrs.emplace_back(rewriter.getStringAttr("comparison_direction"), tmpAttr);
      }
      if (auto tmpAttr = nativeVar_1) {
        tblgen_attrs.emplace_back(rewriter.getStringAttr("compare_type"), tmpAttr);
      }
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(0)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_CompareOp_2 = rewriter.create<::mlir::stablehlo::CompareOp>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_CompareOp_2.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }

    rewriter.replaceOp(op0, tblgen_repl_values);
    return ::mlir::success();
  };
};

/* Generated from:
    external/stablehlo/stablehlo/transforms/ChloDecompositionPatterns.td:357
*/
struct GeneratedConvert12 : public ::mlir::RewritePattern {
  GeneratedConvert12(::mlir::MLIRContext *context)
      : ::mlir::RewritePattern("chlo.is_neg_inf", 1, context, {"stablehlo.compare"}) {}
  ::mlir::LogicalResult matchAndRewrite(::mlir::Operation *op0,
      ::mlir::PatternRewriter &rewriter) const override {
    // Variables for capturing values and attributes used while creating ops
    ::mlir::Operation::operand_range input(op0->getOperands());
    ::llvm::SmallVector<::mlir::Operation *, 4> tblgen_ops;

    // Match
    tblgen_ops.push_back(op0);
    auto castedOp0 = ::llvm::dyn_cast<::mlir::chlo::IsNegInfOp>(op0); (void)castedOp0;
    if(::mlir::failed(__mlir_ods_local_type_constraint_ChloDecompositionPatterns0(rewriter, castedOp0, (*castedOp0.getODSOperands(0).begin()).getType(), "operand 0 of op 'chlo.is_neg_inf' failed to satisfy constraint: 'Non-complex element type'"))) {
      return ::mlir::failure();
    }
    input = castedOp0.getODSOperands(0);

    // Rewrite
    auto odsLoc = rewriter.getFusedLoc({tblgen_ops[0]->getLoc()}); (void)odsLoc;
    ::llvm::SmallVector<::mlir::Value, 4> tblgen_repl_values;
    auto nativeVar_0 = ::mlir::stablehlo::getConstantLikeInfValue(rewriter, odsLoc, (*input.begin()), /*negative=*/true); (void)nativeVar_0;
    auto nativeVar_1 = ::mlir::stablehlo::ComparisonTypeAttr(); (void)nativeVar_1;
    ::mlir::stablehlo::CompareOp tblgen_CompareOp_2;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      tblgen_values.push_back((*input.begin()));
      tblgen_values.push_back(nativeVar_0);
      if (auto tmpAttr = ::mlir::stablehlo::ComparisonDirectionAttr::get(rewriter.getContext(), ::mlir::stablehlo::ComparisonDirection::EQ)) {
        tblgen_attrs.emplace_back(rewriter.getStringAttr("comparison_direction"), tmpAttr);
      }
      if (auto tmpAttr = nativeVar_1) {
        tblgen_attrs.emplace_back(rewriter.getStringAttr("compare_type"), tmpAttr);
      }
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(0)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_CompareOp_2 = rewriter.create<::mlir::stablehlo::CompareOp>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_CompareOp_2.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }

    rewriter.replaceOp(op0, tblgen_repl_values);
    return ::mlir::success();
  };
};

/* Generated from:
    external/stablehlo/stablehlo/transforms/ChloDecompositionPatterns.td:367
*/
struct GeneratedConvert13 : public ::mlir::RewritePattern {
  GeneratedConvert13(::mlir::MLIRContext *context)
      : ::mlir::RewritePattern("chlo.tan", 1, context, {"stablehlo.cosine", "stablehlo.divide", "stablehlo.sine"}) {}
  ::mlir::LogicalResult matchAndRewrite(::mlir::Operation *op0,
      ::mlir::PatternRewriter &rewriter) const override {
    // Variables for capturing values and attributes used while creating ops
    ::mlir::Operation::operand_range input(op0->getOperands());
    ::llvm::SmallVector<::mlir::Operation *, 4> tblgen_ops;

    // Match
    tblgen_ops.push_back(op0);
    auto castedOp0 = ::llvm::dyn_cast<::mlir::chlo::TanOp>(op0); (void)castedOp0;
    if(::mlir::failed(__mlir_ods_local_type_constraint_ChloDecompositionPatterns0(rewriter, castedOp0, (*castedOp0.getODSOperands(0).begin()).getType(), "operand 0 of op 'chlo.tan' failed to satisfy constraint: 'Non-complex element type'"))) {
      return ::mlir::failure();
    }
    input = castedOp0.getODSOperands(0);

    // Rewrite
    auto odsLoc = rewriter.getFusedLoc({tblgen_ops[0]->getLoc()}); (void)odsLoc;
    ::llvm::SmallVector<::mlir::Value, 4> tblgen_repl_values;
    ::mlir::stablehlo::SineOp tblgen_SineOp_0;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      tblgen_SineOp_0 = rewriter.create<::mlir::stablehlo::SineOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::CosineOp tblgen_CosineOp_1;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      tblgen_CosineOp_1 = rewriter.create<::mlir::stablehlo::CosineOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::DivOp tblgen_DivOp_2;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      tblgen_values.push_back((*tblgen_SineOp_0.getODSResults(0).begin()));
      tblgen_values.push_back((*tblgen_CosineOp_1.getODSResults(0).begin()));
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(0)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_DivOp_2 = rewriter.create<::mlir::stablehlo::DivOp>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_DivOp_2.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }

    rewriter.replaceOp(op0, tblgen_repl_values);
    return ::mlir::success();
  };
};

/* Generated from:
    external/stablehlo/stablehlo/transforms/ChloDecompositionPatterns.td:376
*/
struct GeneratedConvert14 : public ::mlir::RewritePattern {
  GeneratedConvert14(::mlir::MLIRContext *context)
      : ::mlir::RewritePattern("chlo.tan", 1, context, {"chlo.tan", "stablehlo.complex", "stablehlo.divide", "stablehlo.imag", "stablehlo.multiply", "stablehlo.negate", "stablehlo.real", "stablehlo.tanh"}) {}
  ::mlir::LogicalResult matchAndRewrite(::mlir::Operation *op0,
      ::mlir::PatternRewriter &rewriter) const override {
    // Variables for capturing values and attributes used while creating ops
    ::mlir::Operation::operand_range input(op0->getOperands());
    ::llvm::SmallVector<::mlir::Operation *, 4> tblgen_ops;

    // Match
    tblgen_ops.push_back(op0);
    auto castedOp0 = ::llvm::dyn_cast<::mlir::chlo::TanOp>(op0); (void)castedOp0;
    if(::mlir::failed(__mlir_ods_local_type_constraint_ChloDecompositionPatterns1(rewriter, castedOp0, (*castedOp0.getODSOperands(0).begin()).getType(), "operand 0 of op 'chlo.tan' failed to satisfy constraint: 'Complex element type'"))) {
      return ::mlir::failure();
    }
    input = castedOp0.getODSOperands(0);

    // Rewrite
    auto odsLoc = rewriter.getFusedLoc({tblgen_ops[0]->getLoc()}); (void)odsLoc;
    ::llvm::SmallVector<::mlir::Value, 4> tblgen_repl_values;
    ::mlir::stablehlo::RealOp tblgen_RealOp_0;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      tblgen_RealOp_0 = rewriter.create<::mlir::stablehlo::RealOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::chlo::TanOp tan;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_RealOp_0.getODSResults(0).begin());
      tan = rewriter.create<::mlir::chlo::TanOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::ImagOp tblgen_ImagOp_1;
    {
      ::mlir::Value tblgen_value_0 = (*input.begin());
      tblgen_ImagOp_1 = rewriter.create<::mlir::stablehlo::ImagOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::TanhOp tanh;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_ImagOp_1.getODSResults(0).begin());
      tanh = rewriter.create<::mlir::stablehlo::TanhOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::ComplexOp tblgen_ComplexOp_2;
    {
      ::mlir::Value tblgen_value_0 = (*tan.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = (*tanh.getODSResults(0).begin());
      tblgen_ComplexOp_2 = rewriter.create<::mlir::stablehlo::ComplexOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    auto nativeVar_3 = ::mlir::stablehlo::getConstantLike(rewriter, odsLoc, 1.0, (*tan.getODSResults(0).begin())); (void)nativeVar_3;
    ::mlir::stablehlo::MulOp tblgen_MulOp_4;
    {
      ::mlir::Value tblgen_value_0 = (*tan.getODSResults(0).begin());
      ::mlir::Value tblgen_value_1 = (*tanh.getODSResults(0).begin());
      tblgen_MulOp_4 = rewriter.create<::mlir::stablehlo::MulOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::NegOp tblgen_NegOp_5;
    {
      ::mlir::Value tblgen_value_0 = (*tblgen_MulOp_4.getODSResults(0).begin());
      tblgen_NegOp_5 = rewriter.create<::mlir::stablehlo::NegOp>(odsLoc,
        /*operand=*/tblgen_value_0
      );
    }
    ::mlir::stablehlo::ComplexOp tblgen_ComplexOp_6;
    {
      ::mlir::Value tblgen_value_0 = nativeVar_3;
      ::mlir::Value tblgen_value_1 = (*tblgen_NegOp_5.getODSResults(0).begin());
      tblgen_ComplexOp_6 = rewriter.create<::mlir::stablehlo::ComplexOp>(odsLoc,
        /*lhs=*/tblgen_value_0,
        /*rhs=*/tblgen_value_1
      );
    }
    ::mlir::stablehlo::DivOp tblgen_DivOp_7;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      tblgen_values.push_back((*tblgen_ComplexOp_2.getODSResults(0).begin()));
      tblgen_values.push_back((*tblgen_ComplexOp_6.getODSResults(0).begin()));
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(0)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_DivOp_7 = rewriter.create<::mlir::stablehlo::DivOp>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_DivOp_7.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }

    rewriter.replaceOp(op0, tblgen_repl_values);
    return ::mlir::success();
  };
};

/* Generated from:
    external/stablehlo/stablehlo/transforms/ChloDecompositionPatterns.td:386
*/
struct GeneratedConvert15 : public ::mlir::RewritePattern {
  GeneratedConvert15(::mlir::MLIRContext *context)
      : ::mlir::RewritePattern("chlo.constant", 1, context, {"stablehlo.constant"}) {}
  ::mlir::LogicalResult matchAndRewrite(::mlir::Operation *op0,
      ::mlir::PatternRewriter &rewriter) const override {
    // Variables for capturing values and attributes used while creating ops
    ::mlir::ElementsAttr v;
    ::llvm::SmallVector<::mlir::Operation *, 4> tblgen_ops;

    // Match
    tblgen_ops.push_back(op0);
    auto castedOp0 = ::llvm::dyn_cast<::mlir::chlo::ConstantOp>(op0); (void)castedOp0;
    {
      auto tblgen_attr = op0->getAttrOfType<::mlir::ElementsAttr>("value");(void)tblgen_attr;
      if (!(tblgen_attr)){
        return rewriter.notifyMatchFailure(op0, [&](::mlir::Diagnostic &diag) {
          diag << "expected op 'chlo.constant' to have attribute 'value' of type '::mlir::ElementsAttr'";
        });
      }
      v = tblgen_attr;
    }

    // Rewrite
    auto odsLoc = rewriter.getFusedLoc({tblgen_ops[0]->getLoc()}); (void)odsLoc;
    ::llvm::SmallVector<::mlir::Value, 4> tblgen_repl_values;
    ::mlir::stablehlo::ConstantOp tblgen_ConstantOp_0;
    {
      ::llvm::SmallVector<::mlir::Value, 4> tblgen_values; (void)tblgen_values;
      ::llvm::SmallVector<::mlir::NamedAttribute, 4> tblgen_attrs; (void)tblgen_attrs;
      if (auto tmpAttr = v) {
        tblgen_attrs.emplace_back(rewriter.getStringAttr("value"), tmpAttr);
      }
      ::llvm::SmallVector<::mlir::Type, 4> tblgen_types; (void)tblgen_types;
      for (auto v: castedOp0.getODSResults(0)) {
        tblgen_types.push_back(v.getType());
      }
      tblgen_ConstantOp_0 = rewriter.create<::mlir::stablehlo::ConstantOp>(odsLoc, tblgen_types, tblgen_values, tblgen_attrs);
    }

    for (auto v: ::llvm::SmallVector<::mlir::Value, 4>{ tblgen_ConstantOp_0.getODSResults(0) }) {
      tblgen_repl_values.push_back(v);
    }

    rewriter.replaceOp(op0, tblgen_repl_values);
    return ::mlir::success();
  };
};

void LLVM_ATTRIBUTE_UNUSED populateWithGenerated(::mlir::RewritePatternSet &patterns) {
  patterns.add<GeneratedConvert0>(patterns.getContext());
  patterns.add<GeneratedConvert1>(patterns.getContext());
  patterns.add<GeneratedConvert2>(patterns.getContext());
  patterns.add<GeneratedConvert3>(patterns.getContext());
  patterns.add<GeneratedConvert4>(patterns.getContext());
  patterns.add<GeneratedConvert5>(patterns.getContext());
  patterns.add<GeneratedConvert6>(patterns.getContext());
  patterns.add<GeneratedConvert7>(patterns.getContext());
  patterns.add<GeneratedConvert8>(patterns.getContext());
  patterns.add<GeneratedConvert9>(patterns.getContext());
  patterns.add<GeneratedConvert10>(patterns.getContext());
  patterns.add<GeneratedConvert11>(patterns.getContext());
  patterns.add<GeneratedConvert12>(patterns.getContext());
  patterns.add<GeneratedConvert13>(patterns.getContext());
  patterns.add<GeneratedConvert14>(patterns.getContext());
  patterns.add<GeneratedConvert15>(patterns.getContext());
}
