package com.spazzmania.archive;

public enum SyncJobErrorHandling {
	IGNORE_ERROR,
	EXIT_QUIETLY_ON_VETO,
	EXIT_QUIETLY_ON_ERROR,
	EXIT_WITH_ERROR_ON_VETO,
	EXIT_WITH_ERROR_ON_ERROR;
}
