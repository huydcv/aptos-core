// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

import { useDisclosure } from '@chakra-ui/react';
import constate from 'constate';
import { CoinTransferFormData } from 'core/components/TransferFlow';
import { useCoinTransferSimulation } from 'core/mutations/transaction';
import { useAccountExists, useAccountOctaCoinBalance } from 'core/queries/account';
import { formatAddress, isAddressValid } from 'core/utils/address';
import { formatCoin, OCTA_POSITIVE_EXPONENT } from 'core/utils/coin';
import { useCallback, useMemo, useState } from 'react';
import { useForm } from 'react-hook-form';
import { parseMoveAbortDetails } from 'shared/move';
import { useActiveAccount } from './useAccounts';
import useDebounce from './useDebounce';

export enum TransferDrawerPage {
  ADD_ADDRESS_AND_AMOUNT = 'Add an address and amount',
  CONFIRM_TRANSACTION = 'Confirm transaction',
}

export const [TransferFlowProvider, useTransferFlow] = constate(() => {
  // hooks
  const formMethods = useForm<CoinTransferFormData>();
  const {
    isOpen: isDrawerOpen,
    onClose: closeDrawer,
    onOpen: openDrawer,
  } = useDisclosure();

  const { activeAccountAddress } = useActiveAccount();
  const { data: coinBalanceOcta } = useAccountOctaCoinBalance(activeAccountAddress);
  const coinBalanceApt = useMemo(() => formatCoin(coinBalanceOcta), [coinBalanceOcta]);

  const [
    transferDrawerPage,
    setTransferDrawerPage,
  ] = useState<TransferDrawerPage>(TransferDrawerPage.ADD_ADDRESS_AND_AMOUNT);

  // form data and methods
  const { watch } = formMethods;
  const amount = watch('amount');
  const recipient = watch('recipient');

  const validRecipientAddress = isAddressValid(recipient) ? formatAddress(recipient) : undefined;
  const { data: doesRecipientAccountExist } = useAccountExists({
    address: validRecipientAddress,
  });

  const amountAptNumber = parseFloat(amount || '0');
  const amountOctaNumber = parseInt((amountAptNumber * OCTA_POSITIVE_EXPONENT).toString(), 10);
  const {
    debouncedValue: debouncedNumberAmountOcta,
    isLoading: debouncedAmountIsLoading,
  } = useDebounce(amountOctaNumber, 500);
  const isBalanceEnoughBeforeFee = (debouncedNumberAmountOcta && coinBalanceOcta !== undefined)
    ? debouncedNumberAmountOcta <= coinBalanceOcta
    : undefined;

  const {
    data: simulationResult,
    error: simulationError,
  } = useCoinTransferSimulation({
    doesRecipientExist: doesRecipientAccountExist,
    octaAmount: debouncedNumberAmountOcta,
    recipient: validRecipientAddress,
  }, {
    enabled: isDrawerOpen && isBalanceEnoughBeforeFee,
    maxGasOctaAmount: coinBalanceOcta || 0,
    refetchInterval: 5000,
  });

  const estimatedGasFeeOcta = debouncedNumberAmountOcta
   && simulationResult
   && Number(simulationResult.gas_used);

  const estimatedGasFeeApt = useMemo(
    () => formatCoin(estimatedGasFeeOcta, { decimals: 8 }),
    [estimatedGasFeeOcta],
  );

  const simulationAbortDetails = simulationResult !== undefined
    ? parseMoveAbortDetails(simulationResult.vm_status)
    : undefined;

  const shouldBalanceShake = isBalanceEnoughBeforeFee === false
  || simulationError !== null
  || simulationAbortDetails !== undefined;

  const canSubmitForm = validRecipientAddress !== undefined
    && !debouncedAmountIsLoading
    && doesRecipientAccountExist !== undefined
    && debouncedNumberAmountOcta !== undefined
    && debouncedNumberAmountOcta >= 0
    && simulationResult?.success === true
    && !simulationError;

  // transfer page state

  const nextOnClick = useCallback(() => {
    setTransferDrawerPage(TransferDrawerPage.CONFIRM_TRANSACTION);
  }, []);

  const backOnClick = useCallback(() => {
    setTransferDrawerPage(TransferDrawerPage.ADD_ADDRESS_AND_AMOUNT);
  }, []);

  return {
    amountAptNumber,
    amountOctaNumber,
    backOnClick,
    canSubmitForm,
    closeDrawer,
    coinBalanceApt,
    coinBalanceOcta,
    doesRecipientAccountExist,
    estimatedGasFeeApt,
    estimatedGasFeeOcta,
    formMethods,
    isDrawerOpen,
    nextOnClick,
    openDrawer,
    shouldBalanceShake,
    simulationResult,
    transferDrawerPage,
    validRecipientAddress,
  };
});
