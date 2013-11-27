/**
 * Copyright (C) 2013 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.runtime.common.client.defaults;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.RuntimeError;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default event handler for REEF RuntimeError: rethrow the exception.
 */
@Provided
@ClientSide
public final class DefaultRuntimeErrorHandler implements EventHandler<RuntimeError> {

  private static final Logger LOG = Logger.getLogger(DefaultRuntimeErrorHandler.class.getName());

  @Inject
  private DefaultRuntimeErrorHandler() {
  }

  @Override
  public void onNext(final RuntimeError error) {
    LOG.log(Level.SEVERE, "Runtime error: " + error, error.getCause());
    throw new RuntimeException("REEF runtime error: " + error, error.asError());
  }
}
