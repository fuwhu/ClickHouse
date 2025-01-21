SELECT mapExtractKeyMultiLike(map('k1-1', 1, 'k2-1', 2), ['k1.*']);
SELECT mapExtractKeyMultiLike(map('k1-1', 1, 'k2-1', 2), ['k2.*']);
SELECT mapExtractKeyMultiLike(map('k1-1', 1, 'k2-1', 2), ['k1.*', 'k2.*']);
SELECT mapExtractKeyMultiLike(map('k1-1', 1, 'k2-1', 2), ['k3.*']);
